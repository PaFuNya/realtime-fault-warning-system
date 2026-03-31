package org.example.tasks

import org.apache.spark.sql.functions._
import redis.clients.jedis.Jedis
import java.sql.Timestamp

/** 任务组 5：实时指标与推理 (Spark -> ClickHouse)
  *   18. 实时工艺参数偏离监控 逻辑：对比实时电流/转速与标准值的偏差，持续偏离 30 秒即记录。
  */
object Task18_ProcessDeviation {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession("Task18_ProcessDeviation")

    // 1. 读取 Sensor 流并添加水位线
    val sensorRawDF = SparkUtils
      .getSensorRawStream(spark)
      .withWatermark("timestamp", "1 minute")

    // 2. 使用 foreachBatch 查询 Redis 并处理 30 秒偏离
    sensorRawDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          val sparkSession = batchDF.sparkSession
          import sparkSession.implicits._

          // 使用 mapPartitions 连接 Redis 获取该设备的标准参数并对比
          val devDF = batchDF
            .mapPartitions { iter =>
              val jedis = new Jedis("127.0.0.1", 6379)
              val res = iter
                .map { row =>
                  val mid = row.getAs[String]("machine_id")
                  val current = row.getAs[Double]("current")
                  val speed = row.getAs[Double]("speed")

                  val stdCurrent = Option(
                    jedis.hget(s"std_params:$mid", "current")
                  ).map(_.toDouble).getOrElse(30.0)
                  val stdSpeed = Option(jedis.hget(s"std_params:$mid", "speed"))
                    .map(_.toDouble)
                    .getOrElse(3000.0)

                  val currentDev = Math.abs(current - stdCurrent) / stdCurrent
                  val speedDev = Math.abs(speed - stdSpeed) / stdSpeed
                  (mid, currentDev, speedDev, row.getAs[Timestamp]("timestamp"))
                }
                .filter(x => x._2 > 0.1 || x._3 > 0.1) // 偏离阈值 > 10%
              jedis.close()
              res
            }
            .toDF("machine_id", "current_dev", "speed_dev", "timestamp")

          devDF.createOrReplaceTempView("temp_dev")
          // 统计 30 秒内偏离记录数，达到 5 条即视为持续偏离
          val aggDevDF = sparkSession.sql("""
        SELECT machine_id, window.start as start_time, window.end as end_time,
               MAX(current_dev) as max_current_dev, MAX(speed_dev) as max_speed_dev
        FROM (
          SELECT *, window(timestamp, '30 seconds') as window FROM temp_dev
        )
        GROUP BY machine_id, window
        HAVING count(*) >= 5
      """)

          try {
            aggDevDF.write
              .mode("append")
              .jdbc(
                SparkUtils.ckUrl,
                "realtime_process_deviation",
                SparkUtils.getCkProperties()
              )
          } catch { case _: Exception => }
      }
      .option("checkpointLocation", "/tmp/checkpoints/process_dev_standalone")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
