package org.example.tasks

import org.apache.spark.sql.functions._

/** 任务组 4：实时数据入湖 (Spark -> Hudi)
  *   13. 实时传感器流写入 Hudi (MOR 读写分离表，支持异步压缩)
  *   14. 实时日志流写入 Hudi (MOR 读写分离表)
  */
object Task13_14_HudiSink {

  def main(args: Array[String]): Unit = {
    // 解决 HDFS 写入权限问题
    System.setProperty("HADOOP_USER_NAME", "root")

    // 1. 初始化带 Hudi 扩展的 SparkSession
    val spark = SparkUtils.getSparkSession("Task13_14_HudiSink")

    // 2. 读 Sensor 流
    val sensorRawDF = SparkUtils.getSensorRawStream(spark)

    // 3. 读 Log 流
    val logRawDF = SparkUtils.getLogRawStream(spark)

    // 4. 任务 13: 写入 Hudi (MOR)
    val hudiSensorOptions = Map(
      "hoodie.table.name" -> "sensor_detail_realtime",
      "hoodie.datasource.write.recordkey.field" -> "machine_id,ts",
      "hoodie.datasource.write.precombine.field" -> "ts",
      // 修改为 MERGE_ON_READ
      "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
      "hoodie.datasource.write.operation" -> "upsert",
      // 关闭 metadata table，绕过 Windows 下 Hadoop 版本的 NoSuchMethodError
      "hoodie.metadata.enable" -> "false",
      // 开启异步压缩 (对于 Spark Streaming 写 MOR 表，通常需要配置这个来控制小文件和日志文件的合并)
      "hoodie.compact.inline" -> "false",
      "hoodie.compact.async.enable" -> "true",
      "hoodie.compact.inline.max.delta.commits" -> "5"
    )

    sensorRawDF.writeStream
      .format("hudi")
      .options(hudiSensorOptions)
      .option(
        "checkpointLocation",
        "hdfs://master:9000/hudi/checkpoints/sensor_hudi_mor"
      )
      .outputMode("append")
      .start("hdfs://master:9000/hudi/dwd/sensor_detail_realtime")

    // 5. 任务 14: 处理并写入 Hudi (MOR)
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
      // 修改为 MERGE_ON_READ
      "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
      "hoodie.datasource.write.operation" -> "upsert",
      // 关闭 metadata table，绕过 Windows 下 Hadoop 版本的 NoSuchMethodError
      "hoodie.metadata.enable" -> "false",
      "hoodie.compact.inline" -> "false",
      "hoodie.compact.async.enable" -> "true",
      "hoodie.compact.inline.max.delta.commits" -> "5"
    )

    processedLogDF.writeStream
      .format("hudi")
      .options(hudiLogOptions)
      .option(
        "checkpointLocation",
        "hdfs://master:9000/hudi/checkpoints/log_hudi_mor"
      )
      .outputMode("append")
      .start("hdfs://master:9000/hudi/dwd/device_log_realtime")

    spark.streams.awaitAnyTermination()
  }
}
