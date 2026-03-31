package org.example.tasks

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * 模块二：实时数据处理引擎 
 * 2.1 实时数据接入
 * 需求：消费 Kafka sensor_raw 和 log_raw 主题。
 * 数据解析：解析 JSON，提取温度、振动、电流等字段。
 */
object Task2_1_DataIngestion {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Task2_1_DataIngestion")
      .master("local[*]")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("WARN")

    val kafkaBrokers = "100.126.226.67:9092,100.90.72.128:9092,100.123.80.25:9092"

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
      .withWatermark("timestamp", "5 seconds")

    val logSchema = new StructType()
      .add("machine_id", StringType).add("ts", LongType).add("error_code", StringType)
      .add("error_msg", StringType).add("stack_trace", StringType)

    val logRawDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "log_raw")
      .option("startingOffsets", "latest").load()
      .select(from_json(col("value").cast("string"), logSchema).alias("data")).select("data.*")
      .withColumn("timestamp", timestamp_seconds(col("ts") / 1000))
      .withWatermark("timestamp", "5 seconds")

    // 如果独立运行，可以将结果输出到控制台验证
    sensorRawDF.writeStream.format("console").start()
    logRawDF.writeStream.format("console").start()
    
    spark.streams.awaitAnyTermination()
  }
}