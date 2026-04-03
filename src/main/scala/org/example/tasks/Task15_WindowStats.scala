package org.example.tasks

import org.apache.spark.sql.functions._

/** 任务组 5：实时指标与推理 (Spark -> ClickHouse)
  *   15. 实时状态窗口统计 逻辑：1 分钟滚动窗口，计算各设备温度最大值、振动 RMS 均值。
  */
object Task15_WindowStats {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession("Task15_WindowStats")

    // 1. 读取 Kafka 原始数据
    val sensorRawDF = SparkUtils.getSensorRawStream(spark)

    // 2. 假设数据已清洗（这里简化处理，如果需要可以调用 Task2_2 的方法）
    // 为了独立运行，我们直接使用原始数据进行窗口统计
    val windowedStatsDF = sensorRawDF

      // 在做任何 groupBy 之前，必须先通过 withWatermark 定义事件时间和允许延迟
      .withWatermark("timestamp", "1 minute") // 为了快速看到结果，把延迟设为极短
      .groupBy(
        window(col("timestamp"), "1 minute"),
        col("machine_id")
      )
      .agg(
        max("temperature").alias("max_temperature"), // 温度最大值
        // 振动 RMS 均值：先求平方和开根号，再在窗口内求 avg
        avg(
          sqrt(
            col("vibration_x") * col("vibration_x") +
              col("vibration_y") * col("vibration_y") +
              col("vibration_z") * col("vibration_z")
          )
        ).alias("avg_vib_rms")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("machine_id"),
        col("max_temperature"),
        col("avg_vib_rms")
      )

    // 3. 写入 ClickHouse
    windowedStatsDF.writeStream
      .outputMode(
        "update"
      ) // 关键修改：把 append 改成 update，每来一条数据都会更新并输出当前窗口的结果，不需要等窗口关闭！
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          try {
            batchDF.write
              .mode("append") // JDBC 这里依然是 append
              .jdbc(
                SparkUtils.ckUrl,
                "realtime_status_window",
                SparkUtils.getCkProperties()
              )
          } catch { case e: Exception => e.printStackTrace() } // 把异常打印出来
      }
      .option("checkpointLocation", "/tmp/checkpoints/status_window_standalone")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
