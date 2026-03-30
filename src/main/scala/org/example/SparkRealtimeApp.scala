package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object SparkRealtimeApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RealtimeDataProcess")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val kafkaBrokers = "100.126.226.67:9092,100.90.72.128:9092,100.123.80.25:9092"

    // 1. Read sensor_raw
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
      .select(from_json(col("value").cast("string"), sensorSchema).alias("data"))
      .select("data.*")
      .withColumn("timestamp", timestamp_seconds(col("ts") / 1000))

    // 2. Read log_raw
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
      // Task 14: 实时解析报错，标记紧急程度
      .withColumn("severity", when(col("error_code") === "999", "Critical")
                              .when(col("error_code") === "500", "High")
                              .otherwise("Normal"))

    // Task 13: 实时传感器流写入 Hudi
    // 目标：Hudi dwd_hudi.sensor_detail_realtime
    val hudiSensorOptions = Map(
      "hoodie.table.name" -> "sensor_detail_realtime",
      "hoodie.datasource.write.recordkey.field" -> "machine_id,ts",
      "hoodie.datasource.write.precombine.field" -> "ts",
      "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.compact.inline" -> "false",
      "hoodie.compact.async.enable" -> "true"
    )

    val sensorHudiQuery = sensorRawDF.writeStream
      .format("hudi")
      .options(hudiSensorOptions)
      .option("checkpointLocation", "/tmp/checkpoints/sensor_hudi")
      .outputMode("append")
      .start("file:///tmp/hudi_dwd/sensor_detail_realtime")

    // Task 14: 实时日志流写入 Hudi
    // 目标：Hudi dwd_hudi.device_log_realtime
    val hudiLogOptions = Map(
      "hoodie.table.name" -> "device_log_realtime",
      "hoodie.datasource.write.recordkey.field" -> "machine_id,ts",
      "hoodie.datasource.write.precombine.field" -> "ts",
      "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.compact.inline" -> "false",
      "hoodie.compact.async.enable" -> "true"
    )

    val logHudiQuery = logRawDF.writeStream
      .format("hudi")
      .options(hudiLogOptions)
      .option("checkpointLocation", "/tmp/checkpoints/log_hudi")
      .outputMode("append")
      .start("file:///tmp/hudi_dwd/device_log_realtime")

    // Task 15: 实时状态窗口统计 (ClickHouse shtd_ads.realtime_status_window)
    val windowedStatsDF = sensorRawDF
      .withWatermark("timestamp", "2 minutes")
      .groupBy(
        window(col("timestamp"), "1 minute"),
        col("machine_id")
      )
      .agg(
        max("temperature").alias("max_temp"),
        mean(sqrt((col("vibration_x") * col("vibration_x") + col("vibration_y") * col("vibration_y") + col("vibration_z") * col("vibration_z")) / 3)).alias("vibration_rms_mean")
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("machine_id"),
        col("max_temp"),
        col("vibration_rms_mean")
      )

    val ckProperties = new java.util.Properties()
    ckProperties.put("user", "default")
    ckProperties.put("password", "123456")
    val ckUrl = "jdbc:clickhouse://127.0.0.1:8123/shtd_ads"

    val windowStatsQuery = windowedStatsDF.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        try {
          batchDF.write
            .mode("append")
            .jdbc(ckUrl, "realtime_status_window", ckProperties)
        } catch {
          case e: Exception => println(s"Error writing to ClickHouse realtime_status_window: ${e.getMessage}")
        }
      }
      .option("checkpointLocation", "/tmp/checkpoints/status_window")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()

    // Task 16: 实时异常检测与报警 (ClickHouse shtd_ads.realtime_alerts)
    val alertDF = sensorRawDF
      .filter(col("temperature") > 80.0 || col("vibration_x") > 2.0)
      .select(
        current_timestamp().alias("alert_time"),
        col("machine_id"),
        when(col("temperature") > 80.0, "High Temperature")
          .otherwise("Vibration Sudden Increase").alias("alert_type"),
        when(col("temperature") > 80.0, "Cool down immediately")
          .otherwise("Inspect machine balance").alias("suggested_action")
      )

    val alertQuery = alertDF.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        try {
          batchDF.write
            .mode("append")
            .jdbc(ckUrl, "realtime_alerts", ckProperties)
        } catch {
          case e: Exception => println(s"Error writing to ClickHouse realtime_alerts: ${e.getMessage}")
        }
      }
      .option("checkpointLocation", "/tmp/checkpoints/realtime_alerts")
      .start()

    // Task 18: 实时工艺参数偏离监控 (ClickHouse shtd_ads.realtime_process_deviation)
    val deviationDF = sensorRawDF
      .withColumn("current_dev", abs(col("current") - 30.0) / 30.0)
      .withColumn("speed_dev", abs(col("speed") - 3000.0) / 3000.0)
      .filter(col("current_dev") > 0.1 || col("speed_dev") > 0.1)
      .select(
        col("timestamp"),
        col("machine_id"),
        col("current_dev"),
        col("speed_dev")
      )
      .withWatermark("timestamp", "1 minute")
      .groupBy(
        window(col("timestamp"), "30 seconds"),
        col("machine_id")
      )
      .agg(
        max("current_dev").alias("max_current_dev"),
        max("speed_dev").alias("max_speed_dev"),
        count("*").alias("dev_count")
      )
      .filter(col("dev_count") >= 5) // 简化：30秒内有至少5条偏离记录即报警
      .select(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        col("machine_id"),
        col("max_current_dev"),
        col("max_speed_dev")
      )

    val devQuery = deviationDF.writeStream
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        try {
          batchDF.write
            .mode("append")
            .jdbc(ckUrl, "realtime_process_deviation", ckProperties)
        } catch {
          case e: Exception => println(s"Error writing to ClickHouse realtime_process_deviation: ${e.getMessage}")
        }
      }
      .option("checkpointLocation", "/tmp/checkpoints/process_dev")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
