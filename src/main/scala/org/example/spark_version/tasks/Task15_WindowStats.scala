package org.example.spark_version.tasks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 任务组 5：实时指标与推理 (Flink/Spark -> ClickHouse)
 * 15. 实时状态窗口统计
 * 逻辑：1 分钟滚动窗口，计算各设备温度最大值、振动 RMS 均值。
 */
object Task15_WindowStats {

  def startWindowStatsSink(enrichedDF: DataFrame, ckUrl: String, ckProperties: java.util.Properties): Unit = {
    val windowedStatsDF = enrichedDF
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

    windowedStatsDF.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      try {
        batchDF.write.mode("append").jdbc(ckUrl, "realtime_status_window", ckProperties)
      } catch { case _: Exception => }
    }.option("checkpointLocation", "/tmp/checkpoints/status_window").start()
  }
}