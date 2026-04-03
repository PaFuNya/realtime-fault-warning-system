package edu.gz

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkReadMySQL {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    System.setProperty("hadoop.home.dir","D:\\01cmppromy\\hadoop-3.1.3")
    // 创建sparkSession，配置运行环境
    val ss: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.warehouse.dir","hdfs://192.168.91.101:8020/user/hive/warehouse/")
      .appName("spark read mysql")
      .enableHiveSupport()
      .getOrCreate()

    // 创建spark读取mysql相关配置
    val readMySQLDF: DataFrame = ss.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.91.101:3306/ds_pub?characterEncoding=utf8&useSSL=false")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_info")
      .load()


    // 将读取到的数据打印在控制台
    readMySQLDF.show()
    readMySQLDF.write
      .mode(SaveMode.Overwrite)
      .format("hive")
      .saveAsTable("ods.order_info1")

    // 关闭SparkSession
    ss.close()
  }

}
