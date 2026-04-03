package edu.offline.dws

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.util.Properties

object ProvinceAvgConsumption {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    //1、设置sparksession
    val ss:SparkSession=SparkSession.builder()
      .master("local[*]")
      .appName("Hudi avg province")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

    ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
      .createTempView("order_v")
    ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/dim_province")
      .createTempView("province_v")
    ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/dim_region")
      .createTempView("region_v")



    ss.sql(
      """
        |select distinct(provinceid) ,provincename,
        |provinceavgconsumption,
        |region_id,region_name,
        |regionavgconsumption,
        |case
        |when provinceavgconsumption>regionavgconsumption then '高'
        |when provinceavgconsumption<regionavgconsumption then '低'
        |else '相同'
        |end as `comparison`
        |from
        |(select p.id as provinceid,p.name as provincename,
        |r.id as region_id,r.region_name as region_name,
        |avg(final_total_amount) over (partition by province_id) provinceavgconsumption,
        |avg(final_total_amount) over (partition by region_id)  regionavgconsumption
        |from order_v  o
        |join province_v  p on o.province_id=p.id
        |join region_v  r on p.region_id=r.id
        |where year(to_date(o.create_time))=2020 and month(to_date(o.create_time))=4
        |)
        |order by provinceid desc,provinceavgconsumption desc,regionavgconsumption desc
        |limit 5
        |""".stripMargin
    ).createTempView("v")
    val df=ss.sql(
      """
        |select
        |provinceid,
        |provincename,
        |provinceavgconsumption,
        |region_id,
        |region_name,
        |regionavgconsumption,
        |comparison
        |from v
        |""".stripMargin
    )
    df.show()
    val properties:Properties=new Properties()
    properties.put("driver","ru.yandex.clickhouse.ClickHouseDriver")
    properties.put("user","default")
    properties.put("password","")
    properties.put("batchsize","10000")
    properties.put("socket_timeout","300000")

    val URL="jdbc:clickhouse://192.168.91.101:8123/dws"

    df.write
      .mode(SaveMode.Append)
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE,10000)
      .jdbc(URL,"provinceavgcmpregion",properties)

    /*val df=ss.sql(
      """
        |select province_id as provinceid,p.name as provincename,
        |avg(final_total_amount) as provinceavgconsumption
        |from order_v o
        |join province_v p on o.province_id=p.id
        |group by province_id,p.name
        |""".stripMargin
    )*/
    /*val df=ss.sql(
      """
        |select provinceid,p.name as provincename,
        |provinceavgconsumption,region_id,region_name,regionavgconsumption,
        |case
        |when provinceavgconsumption>regionavgconsumption then '高'
        |when provinceavgconsumption<regionavgconsumption then '低'
        |else '相同‘
        |end as comparison
        |from
        |(select provinceid,provincename,provinceavgconsumption,region_id
        |(select province_id as provinceid,p.name as provincename,
        |avg(final_total_amount) as provinceavgconsumption
        |from order_v o
        |join province_v p on o.province_id=p.id
        |group by province_id,p.name)
        |join province_v p2 on provinceid=p2.id) p_avg
        |join
        |(select region_id,region_name,
        |avg(final_total_amount) as regionavgconsumption
        |from order_v o
        |join province_v p on o.province_id=p.id
        |join region_v r on p.region_id=r.id
        |group by region_id,r.region_name) r_avg
        |on p_avg.region_id=r_avg.region_id
        |""".stripMargin
    )*/
   ss.close()
  }

}
