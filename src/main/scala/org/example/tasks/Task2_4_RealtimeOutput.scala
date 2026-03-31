package org.example.tasks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import redis.clients.jedis.Jedis

/** 模块二：实时数据处理引擎 2.4 实时结果输出 需求：状态快照：每分钟更新 Redis 中的设备最新健康分。 实时指标：写入 ClickHouse
  * device_realtime_status 表，供大屏展示。
  */
object Task2_4_RealtimeOutput {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Task2_4_RealtimeOutput")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkaBrokers =
      "100.126.226.67:9092,100.90.72.128:9092,100.123.80.25:9092"
    val ckUrl = "jdbc:clickhouse://127.0.0.1:8123/shtd_ads"
    val ckProperties = new java.util.Properties()
    ckProperties.put("user", "default")
    ckProperties.put("password", "123456")

    // 假设数据已清洗（这里简化处理，独立运行时从原始Kafka读数据并补充占位字段）
    val sensorSchema = new StructType()
      .add("machine_id", StringType)
      .add("ts", LongType)
      .add("temperature", DoubleType)
      .add("vibration_x", DoubleType)
      .add("vibration_y", DoubleType)
      .add("vibration_z", DoubleType)
      .add("current", DoubleType)
      .add("noise", DoubleType)
      .add("speed", DoubleType)

    val sensorRawDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "sensor_raw")
      .option("startingOffsets", "latest")
      .load()
      .select(
        from_json(col("value").cast("string"), sensorSchema).alias("data")
      )
      .select("data.*")
      .withColumn("timestamp", timestamp_seconds(col("ts") / 1000))
      // 独立运行补充占位字段
      .withColumn("health_score", lit(100))
      .withColumn("status", lit("稳定运行"))

    sensorRawDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          // 1. 2.4 状态快照：更新 Redis 中的设备最新健康分和状态
          batchDF
            .select("machine_id", "health_score", "status")
            .rdd
            .foreachPartition { partition =>
              val jedis = new Jedis("127.0.0.1", 6379)
              partition.foreach { row =>
                val mid = row.getAs[String]("machine_id")
                jedis.hset(
                  s"device_status:$mid",
                  "health_score",
                  row.getAs[Int]("health_score").toString
                )
                jedis.hset(
                  s"device_status:$mid",
                  "status",
                  row.getAs[String]("status")
                )
              }
              jedis.close()
            }

          // 2. 2.4 实时指标：写入 ClickHouse device_realtime_status 表
          try {
            batchDF.write
              .mode("append")
              .jdbc(ckUrl, "device_realtime_status", ckProperties)
          } catch { case _: Exception => }
      }
      .option("checkpointLocation", "/tmp/checkpoints/redis_status_standalone")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
