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
      .withWatermark("timestamp", "2 minutes")
      .groupBy(
        window(col("timestamp"), "1 minute"),
        col("machine_id")
      )
      .agg(
        max("temperature").alias("max_temp"),
        mean(col("vibration_x")).alias("vibration_rms_mean")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("machine_id"),
        col("max_temp"),
        col("vibration_rms_mean")
      )

    // 3. 写入 ClickHouse
    windowedStatsDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          try {
            batchDF.write
              .mode("append")
              .jdbc(
                SparkUtils.ckUrl,
                "realtime_status_window",
                SparkUtils.getCkProperties()
              )
          } catch { case _: Exception => }
      }
      .option("checkpointLocation", "/tmp/checkpoints/status_window_standalone")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
