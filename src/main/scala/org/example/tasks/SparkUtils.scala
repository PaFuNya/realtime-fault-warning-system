package org.example.tasks

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/** 通用工具类：封装重复的样板代码，让每个 Task 专注于自己的核心逻辑。
  */
object SparkUtils {
  val kafkaBrokers = AppConfig.getString("kafka.brokers", "master:9092,slave1:9092,slave2:9092")
  val ckUrl = AppConfig.getString("clickhouse.url", "jdbc:clickhouse://master:8123/ldc")

  def getCkProperties(): java.util.Properties = {
    val props = new java.util.Properties()
    props.put("user", "default")
    props.put("password", "") // 恢复默认空密码
    props
  }
  // 获取带 Hudi 配置的 SparkSession
  def getSparkSession(appName: String): SparkSession = {
    // 强制指定 HADOOP_HOME 以解决 Windows 下的 winutils 报错问题
    System.setProperty("hadoop.home.dir", "D:\\hadoop")

    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config(
        "spark.sql.extensions",
        "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
      )
      .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog"
      )
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
  // 统一读取 Sensor Kafka 数据流并解析 JSON
  def getSensorRawStream(spark: SparkSession): DataFrame = {
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
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "sensor_raw")
      .option("startingOffsets", "earliest")
      .load()
      .select(
        from_json(col("value").cast("string"), sensorSchema).alias("data")
      )
      .select("data.*")
      .withColumn("timestamp", timestamp_seconds(col("ts") / 1000))
      // 注：暂时禁用时间过滤，因为数据时间戳与系统时间可能不一致
      // .filter(
      //   col("timestamp") >= expr(s"current_timestamp() - interval ${AppConfig.getInt("max_allowed_lateness_seconds", 30)} seconds")
      // )
  }
  // 统一读取 Log Kafka 数据流并解析 JSON
  def getLogRawStream(spark: SparkSession): DataFrame = {
    val logSchema = new StructType()
      .add("machine_id", StringType)
      .add("ts", LongType)
      .add("error_code", StringType)
      .add("error_msg", StringType)
      .add("stack_trace", StringType)
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "log_raw")
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("string"), logSchema).alias("data"))
      .select("data.*")
      .withColumn("timestamp", timestamp_seconds(col("ts") / 1000))
      // 注：暂时禁用时间过滤，因为数据时间戳与系统时间可能不一致
      // .filter(
      //   col("timestamp") >= expr(s"current_timestamp() - interval ${AppConfig.getInt("max_allowed_lateness_seconds", 30)} seconds")
      // )
  }

  // 获取 ChangeRecord 流 (CSV格式: machine_id, xxx, xxx, status, start_time, end_time, duration)
  def getChangeRecordStream(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "ChangeRecord")
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING) as csv_line")
      .select(
        // 如果数据是 26_267_645,116,30,预警,... 那么第二列 (索引为1) 就是真正的 machine_id
        split(col("csv_line"), ",").getItem(1).alias("machine_id"),
        split(col("csv_line"), ",").getItem(3).alias("status"),
        to_timestamp(
          split(col("csv_line"), ",").getItem(4),
          "yyyy-MM-dd HH:mm:ss"
        ).alias("start_time"),
        to_timestamp(
          split(col("csv_line"), ",").getItem(5),
          "yyyy-MM-dd HH:mm:ss"
        ).alias("end_time")
      )
  }
}
