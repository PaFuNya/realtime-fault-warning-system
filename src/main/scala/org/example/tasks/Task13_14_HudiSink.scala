package org.example.tasks

import org.apache.spark.sql.functions._

/** 任务组 4：实时数据入湖 (Spark -> Hudi)
  *   13. 实时传感器流写入 Hudi (COW 普通表)
  *   14. 实时日志流写入 Hudi (COW 普通表)
  */
object Task13_14_HudiSink {

  def main(args: Array[String]): Unit = {
    // 1. 初始化带 Hudi 扩展的 SparkSession
    val spark = SparkUtils.getSparkSession("Task13_14_HudiSink")

    // 2. 读 Sensor 流
    val sensorRawDF = SparkUtils.getSensorRawStream(spark)

    // 3. 读 Log 流
    val logRawDF = SparkUtils.getLogRawStream(spark)

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
