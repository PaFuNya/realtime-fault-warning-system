package edu.gz

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import java.util.Properties

object provinceConsumptionMysql {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")

    val ss:SparkSession=SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.warehouse.dir","hdfs://192.168.91.101:8020/user/hive/warehouse/")
      .appName("spark write hive")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    ss.sql(
      """
        |select dim_province.id as province_id,
        |dim_province.name as province_name,
        |dim_region.id as region_id,
        |dim_region.region_name,
        |sum(final_total_amount) as total_amount,
        |count(*) as total_count,
        |year(order_info.create_time) as year,
        |month(order_info.create_time) as month
        |from
        |dwd.dim_base_province dim_province join dwd.dim_base_region dim_region
        |on (region_id=dim_region.id)
        |join dwd.order_info
        |on(province_id=dim_province.id)
        |group by dim_province.id,
        |province_name,
        |dim_region.id,
        |dim_region.region_name,
        |year(order_info.create_time),
        |month(order_info.create_time)
        |""".stripMargin).createTempView("v")

    val frame=ss.sql(
      """
        |select province_id,
        |province_name,
        |region_id,
        |region_name,
        |total_amount,
        |total_count,
        |row_number() over(partition by `year`,`month`,`region_id`
        |order by total_amount desc) as `sequence`,
        |year,
        |month
        |from v
        |""".stripMargin)
    frame.printSchema()
    //将数据写出到mysql
    val properties:Properties=new Properties()
    properties.put("driver","com.mysql.jdbc.Driver")
    properties.put("user","root")
    properties.put("password","123456")
    properties.put("batchsize","10000")
    properties.put("socket_timeout","300000")

    val URL="jdbc:mysql://192.168.91.101:3306/dws?characterEncoding=utf8&useSSL=false"

    frame.write
      .mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE,10000)
      .jdbc(URL,"province_consumption_day_aggr",properties)

    ss.close()
  }


}
