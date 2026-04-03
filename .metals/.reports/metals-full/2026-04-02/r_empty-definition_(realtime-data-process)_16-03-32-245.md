error id: file:///D:/Desktop/Match/MATCH/OverMatch/src/main/scala/org/example/tasks/TestReadHudiMOR.scala:local0
file:///D:/Desktop/Match/MATCH/OverMatch/src/main/scala/org/example/tasks/TestReadHudiMOR.scala
empty definition using pc, found symbol in pc: 
found definition using semanticdb; symbol local0
empty definition using fallback
non-local guesses:

offset: 631
uri: file:///D:/Desktop/Match/MATCH/OverMatch/src/main/scala/org/example/tasks/TestReadHudiMOR.scala
text:
```scala
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

    val hudiTablePath = "hdfs://bigdata1:9000/hudi/dwd/sensor_detail_realtime"

    println("==============================================")
    println("1. Snapshot 模式读取 (默认模式，即读时合并，包含最新数据)")
    println("==============================================")
    
    val snapshotDF = spark@@.read
      .format("hudi")
      // 对于 MOR 表，不加额外 option 默认就是 Snapshot 查询
      .load(hudiTablePath)

    // 打印表结构并查看前几条数据
    snapshotDF.printSchema()
    snapshotDF.show(5, truncate = false)


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

```


#### Short summary: 

empty definition using pc, found symbol in pc: 