package org.example.spark_version.tasks

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 任务组 5：实时指标与推理 (Flink/Spark -> ClickHouse)
 * 16. 实时异常检测与报警
 * 源：Kafka sensor_raw + log_raw (经过合并)
 * 目标：ClickHouse shtd_ads.realtime_alerts & Kafka alert_topic (2.4要求)
 * 规则引擎：温度 > 阈值 OR 振动突增 > 20% OR Error Code 999 突增。
 */
object Task16_RealtimeAlerts {

  def startAlertSinks(enrichedDF: DataFrame, logRawDF: DataFrame, ckUrl: String, ckProperties: java.util.Properties, kafkaBrokers: String): Unit = {
    
    // 来自传感器流的异常 (温度 > 80.0 或 振动突增 > 20%)
    val sensorAlerts = enrichedDF.filter(col("temperature") > 80.0 || col("vib_increase_ratio") > 0.2)
      .select(
        col("timestamp").alias("alert_time"),
        col("machine_id"),
        when(col("temperature") > 80.0, "High Temperature").otherwise("Vibration Sudden Increase > 20%").alias("alert_type"),
        lit("Inspect machine immediately").alias("suggested_action")
      )

    // 2.3 日志异常检测: 对 log_raw 进行匹配，发现未知错误模式（如“Error Code 999”）
    val logAlerts = logRawDF.filter(col("error_code") === "999")
      .select(
        col("timestamp").alias("alert_time"),
        col("machine_id"),
        lit("Critical Log Error 999").alias("alert_type"),
        lit("Check hardware logs").alias("suggested_action")
      )

    val combinedAlerts = sensorAlerts.union(logAlerts)

    // 写入 ClickHouse (任务 16)
    combinedAlerts.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      try {
        batchDF.write.mode("append").jdbc(ckUrl, "realtime_alerts", ckProperties)
      } catch { case _: Exception => }
    }.option("checkpointLocation", "/tmp/checkpoints/realtime_alerts_ck").start()

    // 推送至 Kafka alert_topic (模块 2.4)
    combinedAlerts.selectExpr("CAST(machine_id AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", "alert_topic")
      .option("checkpointLocation", "/tmp/checkpoints/realtime_alerts_kafka").start()
  }
}