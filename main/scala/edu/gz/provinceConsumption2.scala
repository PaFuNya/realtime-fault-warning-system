package edu.gz

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

import java.util.Properties

object provinceConsumption2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")

    val ss:SparkSession=SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.warehouse.dir","hdfs://master:9000/user/hive/warehouse/")
      .appName("spark write hive")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    ss.sql(
      """
        |select
        |province as province_name,
        |city as region_name,  -- 假设这里的 region 指的是城市，或者你需要关联真正的地区维表
        |sum(final_total_amount) as total_amount,
        |count(*) as total_count,
        |year(create_time) as year,
        |month(create_time) as month
        |from dwd.order_info
        |where final_total_amount is not null -- 防止空值干扰
        |group by province, city, year(create_time), month(create_time)
        """.stripMargin).createTempView("v")

    val frame=ss.sql(
      """
        select
        |province_name,
        |region_name,
        |total_amount,
        |total_count,
        |row_number() over(partition by year, month, region_name
        |  order by total_amount desc) as sequence,
        |year,
        |month
        |from v
        |""".stripMargin)
    frame.printSchema()

    val properties:Properties=new Properties()
    properties.put("driver","ru.yandex.clickhouse.ClickHouseDriver")
    properties.put("user","default")
    properties.put("password","")
    properties.put("batchsize","10000")
    properties.put("socket_timeout","300000")

    val URL="jdbc:clickhouse://master:8123/dws"

    frame.write
      .mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE,10000)
      .jdbc(URL,"province_consumption_day_aggr",properties)

    ss.close()
  }

}
