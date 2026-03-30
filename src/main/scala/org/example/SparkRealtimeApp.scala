package org.example

import org.apache.spark.sql.SparkSession
import org.example.tasks._

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

    val ckUrl = "jdbc:clickhouse://127.0.0.1:8123/shtd_ads"
    val ckProperties = new java.util.Properties()
    ckProperties.put("user", "default")
    ckProperties.put("password", "123456")

    // 模块 2.1: 实时数据接入 (消费 Kafka 并解析 JSON)
    val sensorRawDF = Task2_1_DataIngestion.getSensorRawStream(spark)
    val logRawDF = Task2_1_DataIngestion.getLogRawStream(spark)

    // 任务 13 & 14: 写入 Hudi (使用 COW 表)
    Task13_14_HudiSink.startHudiSinks(sensorRawDF, logRawDF)

    // 模块 2.1 & 2.2: 流式清洗填补缺失值、状态机识别、提取实时特征 (温度上升斜率, 振动突增率)
    val enrichedDF = Task2_2_FeatureEngineering.getEnrichedStream(spark, sensorRawDF)

    // 模块 2.4: 状态快照 (更新 Redis) & 写入 ClickHouse 设备实时状态表
    Task2_4_RealtimeOutput.startRedisAndClickHouseSink(enrichedDF, ckUrl, ckProperties)

    // 任务 15: 实时状态窗口统计 (1分钟滚动窗口)
    Task15_WindowStats.startWindowStatsSink(enrichedDF, ckUrl, ckProperties)

    // 任务 16 & 模块 2.4: 实时异常检测与报警 (推送至 ClickHouse & Kafka alert_topic)
    Task16_RealtimeAlerts.startAlertSinks(enrichedDF, logRawDF, ckUrl, ckProperties, Task2_1_DataIngestion.kafkaBrokers)

    // 任务 18: 实时工艺参数偏离监控 (查询 Redis 标准参数)
    Task18_ProcessDeviation.startDeviationSink(enrichedDF, ckUrl, ckProperties)

    spark.streams.awaitAnyTermination()
  }
}