package edu.gz

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, date_format, lit}

object SparkReadMySQLWriteHiveDwdClean {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    // 创建sparkSession，配置运行环境
    val ss: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.warehouse.dir","hdfs://192.168.91.101:8020/user/hive/warehouse/")
      .appName("spark read mysql")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    val df:DataFrame=ss.sql("select * from ods.order_info_par")
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_insert_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_modify_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))

    df.write
      .mode(SaveMode.Overwrite)
      .format("hive")
      .partitionBy("etl_date")
      .saveAsTable("dwd.order_info")

    ss.sql("show partitions dwd.order_info").show()
    ss.sql("select * from dwd.order_info limit 2").show()

    ss.close()
  }

}
