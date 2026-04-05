package org.example.tasks

import org.apache.spark.sql.SparkSession

/** 辅助测试任务：验证如何读取清洗后的 Hudi MOR 表
  */
object TestReadHudiMOR {

  def main(args: Array[String]): Unit = {
    // 解决 HDFS 权限问题
    System.setProperty("HADOOP_USER_NAME", "root")

    // 初始化带 Hudi 扩展的 SparkSession
    val spark = SparkUtils.getSparkSession("TestReadHudiMOR")

    val hudiTablePath = "hdfs://master:9000/hudi/dwd/sensor_detail_realtime"

    println("==============================================")
    println("1. Snapshot 模式读取 (默认模式，即读时合并，包含最新数据)")
    println("==============================================")

    val snapshotDF = spark.read
      .format("hudi")
      // 对于 MOR 表，不加额外 option 默认就是 Snapshot 查询
      .load(hudiTablePath)

    // 打印表结构
    snapshotDF.printSchema()

    // 将 DataFrame 注册为临时视图，以便使用 Spark SQL 进行联查
    snapshotDF.createOrReplaceTempView("realtime_sensor")

    println("\n[验证 1] 执行 Spark SQL 查询：获取最新写入的传感器数据")
    // 按时间倒序查询，验证最新一秒的数据是否可见
    spark
      .sql(
        """
        |SELECT machine_id, ts, temperature, vibration_x, vibration_y, vibration_z 
        |FROM realtime_sensor 
        |ORDER BY ts DESC 
        |LIMIT 10
        |""".stripMargin
      )
      .show(truncate = false)

    println("==============================================")
    println("2. Read Optimized 模式读取 (仅读取已压缩的 parquet，不读 log，速度快但有延迟)")
    println("==============================================")

    val roDF = spark.read
      .format("hudi")
      // 指定查询模式为读优化
      .option("hoodie.datasource.query.type", "read_optimized")
      .load(hudiTablePath)

    roDF.show(5, truncate = false)

    // 统计两种模式下的数据量差异（如果数据刚写入还在 log 文件中，两者数量会不同）
    println(s"Snapshot 查询总条数: ${snapshotDF.count()}")
    println(s"Read Optimized 查询总条数: ${roDF.count()}")

    spark.stop()
  }
}
