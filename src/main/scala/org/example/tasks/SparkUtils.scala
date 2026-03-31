package org.example.tasks

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * 通用工具类：封装重复的样板代码，让每个 Task 专注于自己的核心逻辑。
 */
object SparkUtils {

  val kafkaBrokers = "100.126.226.67:9092,100.90.72.128:9092,100.123.80.25:9092"
  val ckUrl = "jdbc:clickhouse://127.0.0.1:8123/shtd_ads"
  
  def getCkProperties(): java.util.Properties = {
    val props = new java.util.Properties()
    props.put("user", "default")
    props.put("password", "123456")
    props
  }

  // 获取带 Hudi 配置的 SparkSession
  def getSparkSession(appName: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  // 统一读取 Sensor Kafka 数据流并解析 JSON
  def getSensorRawStream(spark: SparkSession): DataFrame = {
    val sensorSchema = new StructType()
      .add("machine_id", StringType).add("ts", LongType).add("temperature", DoubleType)
      .add("vibration_x", DoubleType).add("vibration_y", DoubleType).add("vibration_z", DoubleType)
      .add("current", DoubleType).add("noise", DoubleType).add("speed", DoubleType)

    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "sensor_raw")
      .option("startingOffsets", "latest").load()
      .select(from_json(col("value").cast("string"), sensorSchema).alias("data")).select("data.*")
      .withColumn("timestamp", timestamp_seconds(col("ts") / 1000))
  }

  // 统一读取 Log Kafka 数据流并解析 JSON
  def getLogRawStream(spark: SparkSession): DataFrame = {
    val logSchema = new StructType()
      .add("machine_id", StringType).add("ts", LongType).add("error_code", StringType)
      .add("error_msg", StringType).add("stack_trace", StringType)

    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "log_raw")
      .option("startingOffsets", "latest").load()
      .select(from_json(col("value").cast("string"), logSchema).alias("data")).select("data.*")
      .withColumn("timestamp", timestamp_seconds(col("ts") / 1000))
  }
}