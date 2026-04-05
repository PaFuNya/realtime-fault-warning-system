package edu.cmp26

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File

object SparkReadMySQLWriteHive {
  def main(args: Array[String]): Unit = {
    // 设定HDFS的用户为 root
    System.setProperty("HADOOP_USER_NAME","root")
    System.setProperty("hadoop.home.dir","D:\\hadoop-3.2.0")

    // 创建sparkSession，配置运行环境
    val ss:SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.warehouse.dir","hdfs://master:9000/user/hive/warehouse/")
      .config("hive.security.authorization.enabled", "false")
      .appName("spark read mysql")
      .enableHiveSupport() // 在这里增加对hive的支持
      .getOrCreate()

    // 创建spark读取mysql相关配置
    val readMySQLDF:DataFrame = ss.read.format("jdbc")
      .option("url", "jdbc:mysql://master:3306/ds_db01?characterEncoding=utf8&useSSL=false")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_master")
      .load()

    // 通过dataframe 将数据写入到hive
    readMySQLDF.write
      .mode(SaveMode.Overwrite) // 以overwrite的方式写入
      .format("hive")
      .saveAsTable("ods.order_master") //库名.表名

    // 关闭SparkSession
    ss.close()
  }
}
