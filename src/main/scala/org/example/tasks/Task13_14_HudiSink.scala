package org.example.tasks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * 任务组 4：实时数据入湖 (Spark -> Hudi)
 * 13. 实时传感器流写入 Hudi (COW 普通表)
 * 14. 实时日志流写入 Hudi (COW 普通表)
 */
object Task13_14_HudiSink {

  def main(args: Array[String]): Unit = {
    // 1. 初始化带 Hudi 扩展的 SparkSession
    val spark = SparkSession.builder()
      .appName("Task13_14_HudiSink")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("WARN")

    val kafkaBrokers = "100.126.226.67:9092,100.90.72.128:9092,100.123.80.25:9092"

    // 2. 读 Sensor 流
    val sensorSchema = new StructType()
      .add("machine_id", StringType).add("ts", LongType).add("temperature", DoubleType)
      .add("vibration_x", DoubleType).add("vibration_y", DoubleType).add("vibration_z", DoubleType)
      .add("current", DoubleType).add("noise", DoubleType).add("speed", DoubleType)

    val sensorRawDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "sensor_raw")
      .option("startingOffsets", "latest").load()
      .select(from_json(col("value").cast("string"), sensorSchema).alias("data")).select("data.*")
      .withColumn("timestamp", timestamp_seconds(col("ts") / 1000))

    // 3. 读 Log 流
    val logSchema = new StructType()
      .add("machine_id", StringType).add("ts", LongType).add("error_code", StringType)
      .add("error_msg", StringType).add("stack_trace", StringType)

    val logRawDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "log_raw")
      .option("startingOffsets", "latest").load()
      .select(from_json(col("value").cast("string"), logSchema).alias("data")).select("data.*")
      .withColumn("timestamp", timestamp_seconds(col("ts") / 1000))

    // 4. 任务 13: 写入 Hudi (COW)
    val hudiSensorOptions = Map(
      "hoodie.table.name" -> "sensor_detail_realtime",
      "hoodie.datasource.write.recordkey.field" -> "machine_id,ts",
      "hoodie.datasource.write.precombine.field" -> "ts",
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.operation" -> "upsert"
    )

    sensorRawDF.writeStream
      .format("hudi")
      .options(hudiSensorOptions)
      .option("checkpointLocation", "/tmp/checkpoints/sensor_hudi_cow")
      .outputMode("append")
      .start("file:///tmp/hudi_dwd/sensor_detail_realtime_cow")

    // 5. 任务 14: 处理并写入 Hudi (COW)
    val processedLogDF = logRawDF.withColumn(
      "severity",
      when(col("error_code") === "999", "Critical")
      .when(col("error_code") === "500", "High")
      .otherwise("Normal")
    )

    val hudiLogOptions = Map(
      "hoodie.table.name" -> "device_log_realtime",
      "hoodie.datasource.write.recordkey.field" -> "machine_id,ts",
      "hoodie.datasource.write.precombine.field" -> "ts",
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.operation" -> "upsert"
    )

    processedLogDF.writeStream
      .format("hudi")
      .options(hudiLogOptions)
      .option("checkpointLocation", "/tmp/checkpoints/log_hudi_cow")
      .outputMode("append")
      .start("file:///tmp/hudi_dwd/device_log_realtime_cow")
      
    spark.streams.awaitAnyTermination()
  }
}