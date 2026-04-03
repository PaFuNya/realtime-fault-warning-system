package edu.offline.dws

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME

object ProvinceConsumption {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    //1.创建SparkSession
    val ss:SparkSession=SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Hudi consum province")
      .enableHiveSupport()
      .getOrCreate()

    ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
      .createTempView("order_info_v")
    ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/dim_province")
      .createTempView("province_v")
    ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/dim_region")
      .createTempView("region_v")
    val df=ss.sql(
      """
        |SELECT
        |province_id,p.name as province_name,region_id,region_name,
        |total_amount,total_count,
        |ROW_NUMBER() over(partition by year,month,region_id order by total_amount desc)  sequence,
        |year,month
        |FROM
        |(SELECT
        |province_id,sum(final_total_amount) as total_amount,count(*) as total_count,
        |year(to_date(create_time)) as year,month(to_date(create_time)) as month
        |FROM order_info_v
        |GROUP BY
        |province_id,year(to_date(create_time)),month(to_date(create_time))
        |) v1
        |join province_v p on v1.province_id=p.id
        |join region_v r on p.region_id=r.id
        |""".stripMargin
    )
      .selectExpr("uuid() as uuid","*")
    df.show()
    df.write
      .mode(SaveMode.Overwrite)
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(),"total_count")
      .option(RECORDKEY_FIELD.key(),"uuid")
      .option(PARTITIONPATH_FIELD.key(),"year,month")
      .option(TBL_NAME.key(),"province_consumption_day_aggr")
      .save("hdfs://master:8020/user/hive/warehouse/dws_ds_hudi.db/province_consumption_day_aggr")
    ss.close()
  }

}
