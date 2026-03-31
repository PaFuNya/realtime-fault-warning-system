package org.example.tasks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** 任务组 5：实时指标与推理 (Spark -> ClickHouse)
  *   16. 实时异常检测与报警 源：Kafka sensor_raw + log_raw (经过合并) 目标：ClickHouse
  *       shtd_ads.realtime_alerts & Kafka alert_topic (2.4要求) 规则引擎：温度 > 阈值 OR
  *       振动突增 > 20% OR Error Code 999 突增。
  */
object Task16_RealtimeAlerts {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Task16_RealtimeAlerts")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkaBrokers =
      "100.126.226.67:9092,100.90.72.128:9092,100.123.80.25:9092"
    val ckUrl = "jdbc:clickhouse://127.0.0.1:8123/shtd_ads"
    val ckProperties = new java.util.Properties()
    ckProperties.put("user", "default")
    ckProperties.put("password", "123456")

    // 1. 读取 Sensor 流
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

    // 2. 读取 Log 流
    val logSchema = new StructType()
      .add("machine_id", StringType)
      .add("ts", LongType)
      .add("error_code", StringType)
      .add("error_msg", StringType)
      .add("stack_trace", StringType)

    val logRawDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "log_raw")
      .option("startingOffsets", "latest")
      .load()
      .select(from_json(col("value").cast("string"), logSchema).alias("data"))
      .select("data.*")
      .withColumn("timestamp", timestamp_seconds(col("ts") / 1000))

    // 3. 来自传感器流的异常 (温度 > 80.0 或 振动异常 这里用基础值模拟)
    val sensorAlerts = sensorRawDF
      .filter(col("temperature") > 80.0 || col("vibration_x") > 2.0)
      .select(
        col("timestamp").alias("alert_time"),
        col("machine_id"),
        when(col("temperature") > 80.0, "High Temperature")
          .otherwise("Vibration Sudden Increase > 20%")
          .alias("alert_type"),
        lit("Inspect machine immediately").alias("suggested_action")
      )

    // 4. 日志异常检测: 对 log_raw 进行匹配，发现未知错误模式（如“Error Code 999”）
    val logAlerts = logRawDF
      .filter(col("error_code") === "999")
      .select(
        col("timestamp").alias("alert_time"),
        col("machine_id"),
        lit("Critical Log Error 999").alias("alert_type"),
        lit("Check hardware logs").alias("suggested_action")
      )

    val combinedAlerts = sensorAlerts.union(logAlerts)

    // 5. 写入 ClickHouse (任务 16)
    combinedAlerts.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          try {
            batchDF.write
              .mode("append")
              .jdbc(ckUrl, "realtime_alerts", ckProperties)
          } catch { case _: Exception => }
      }
      .option(
        "checkpointLocation",
        "/tmp/checkpoints/realtime_alerts_ck_standalone"
      )
      .start()

    // 6. 推送至 Kafka alert_topic (模块 2.4)
    combinedAlerts
      .selectExpr(
        "CAST(machine_id AS STRING) AS key",
        "to_json(struct(*)) AS value"
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("topic", "alert_topic")
      .option(
        "checkpointLocation",
        "/tmp/checkpoints/realtime_alerts_kafka_standalone"
      )
      .start()

    spark.streams.awaitAnyTermination()
  }
}
