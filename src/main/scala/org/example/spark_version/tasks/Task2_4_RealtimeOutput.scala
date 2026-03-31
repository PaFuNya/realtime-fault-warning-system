package org.example.spark_version.tasks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import redis.clients.jedis.Jedis

/**
 * 模块二：实时数据处理引擎
 * 2.4 实时结果输出
 * 需求：状态快照：每分钟更新 Redis 中的设备最新健康分。
 * 实时指标：写入 ClickHouse device_realtime_status 表，供大屏展示。
 */
object Task2_4_RealtimeOutput {
  
  def startRedisAndClickHouseSink(enrichedDF: DataFrame, ckUrl: String, ckProperties: java.util.Properties): Unit = {
    enrichedDF.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      // 1. 2.4 状态快照：更新 Redis 中的设备最新健康分和状态
      batchDF.select("machine_id", "health_score", "status").rdd.foreachPartition { partition =>
        val jedis = new Jedis("127.0.0.1", 6379)
        partition.foreach { row =>
          val mid = row.getAs[String]("machine_id")
          jedis.hset(s"device_status:$mid", "health_score", row.getAs[Int]("health_score").toString)
          jedis.hset(s"device_status:$mid", "status", row.getAs[String]("status"))
        }
        jedis.close()
      }

      // 2. 2.4 实时指标：写入 ClickHouse device_realtime_status 表
      try {
        batchDF.write.mode("append").jdbc(ckUrl, "device_realtime_status", ckProperties)
      } catch { case _: Exception => }
    }.option("checkpointLocation", "/tmp/checkpoints/redis_status").start()
  }
}