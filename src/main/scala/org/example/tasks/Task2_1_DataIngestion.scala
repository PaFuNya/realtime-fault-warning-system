package org.example.tasks

/** 模块二：实时数据处理引擎 2.1 实时数据接入 需求：消费 Kafka sensor_raw 和 log_raw 主题。 数据解析：解析
  * JSON，提取温度、振动、电流等字段。
  */
object Task2_1_DataIngestion {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession("Task2_1_DataIngestion")

    // 直接调用工具类读取并解析
    val sensorRawDF = SparkUtils
      .getSensorRawStream(spark)
      .withWatermark("timestamp", "5 seconds")
    val logRawDF =
      SparkUtils.getLogRawStream(spark).withWatermark("timestamp", "5 seconds")

    // 如果独立运行，可以将结果输出到控制台验证
    sensorRawDF.writeStream.format("console").start()
    logRawDF.writeStream.format("console").start()

    spark.streams.awaitAnyTermination()
  }
}
