package edu.gz

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

object SparkReadMySQLWriteHiveProvincePar {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val ss:SparkSession=SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.warehouse.dir","hdfs://192.168.91.101:8020/user/hive/warehouse/")
      .appName("spark write hive")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    val readMySQLDF:DataFrame=ss.read.format("jdbc")
      .option("url","jdbc:mysql://192.168.91.101:3306/ds_pub?characterEncoding=utf8&useSSL=false")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user","root")
      .option("password","123456")
      .option("dbtable","base_province")
      .load()

    val readMySQLDFwithCol:DataFrame=
      readMySQLDF.withColumn("etl_date",lit("20240413"))

    readMySQLDFwithCol.write
      .mode(SaveMode.Overwrite)
      .format("hive")
      .partitionBy("etl_date")
      .saveAsTable("dwd.dim_base_province")

    ss.close()
  }

}
