package edu.cmp26

import org.apache.spark.sql.SparkSession

object SparkReadMySQL {
  def main(args: Array[String]): Unit = {
    // 创建sparkSession，配置运⾏环境
    val ss: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("spark read mysql")
      .getOrCreate()
    var readMySQLDF=ss.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.45.11:3306/ds_db01?characterEncoding=utf8&useSSL=false")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_master")
      .load()
    readMySQLDF.show()
    ss.close()
  }

}
