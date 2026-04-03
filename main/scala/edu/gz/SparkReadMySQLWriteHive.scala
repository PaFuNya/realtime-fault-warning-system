package edu.gz

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkReadMySQLWriteHive {
  def main(args: Array[String]): Unit = {
    // 设定HDFS的用户为 root
    //System.setProperty("HADOOP_USER_NAME","root")
    //System.setProperty("hadoop.home.dir","D:\\01cmppromy\\hadoop-3.1.3")
    // 创建sparkSession，配置运行环境
    val ss:SparkSession = SparkSession.builder()
      //.master("local[*]")
      .config("spark.sql.warehouse.dir","hdfs://192.168.1.100:8020/user/hive/warehouse/")
      .appName("spark read mysql")
      .enableHiveSupport() // 在这里增加对hive的支持
      .getOrCreate()

    // 创建spark读取mysql相关配置
    val readMySQLDF:DataFrame = ss.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.1.100:3306/ds_pub?characterEncoding=utf8&useSSL=false")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_info")
      .load()

    // 通过dataframe 将数据写入到hive
    readMySQLDF.write
      .mode(SaveMode.Overwrite) // 以overwrite的方式写入
      .format("hive")
      .saveAsTable("ods.order_info") //库名.表名

          // 关闭SparkSession
    ss.close()
  }
}
