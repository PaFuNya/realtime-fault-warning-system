package edu.offline.dws

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.sql.{SaveMode, SparkSession}
//每人每天下单的数量和下单的总金额
object UserConsumption {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    // 1、创建sparkSession，配置运行环境
    val ss=SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Hudi user consum")
      .enableHiveSupport()
      .getOrCreate()

    ss.read.format("hudi")
    .load("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info")
      .createTempView("order_info_v")
    ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")
      .createTempView("user_info_v")

    val df=ss.sql(
      """
        |SELECT
        |  o.user_id as user_id,
        |  u.login_name as user_name,
        |  sum(final_total_amount) as total_amount,
        |  count(o.id) as total_count,
        |  year(to_date(o.create_time)) as year,
        |  month(to_date(o.create_time)) as month,
        |  dayofmonth(to_date(o.create_time)) as day
        |FROM
        |  order_info_v as o join user_info_v as u
        |  on o.user_id=u.id
        |GROUP BY
        |  user_id,
        |  user_name,
        |  year(to_date(o.create_time)),
        |  month(to_date(o.create_time)),
        |  dayofmonth(to_date(o.create_time))
        |""".stripMargin
    )
      .selectExpr("uuid() as uuid","*")
    df.show()
    //3、写入hudi，配置Hudi的写入选项，包括主键、预聚合字段、分区等。
    df.write
      .mode(SaveMode.Overwrite)
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(),"total_count")
      .option(RECORDKEY_FIELD.key(),"uuid")
      .option(PARTITIONPATH_FIELD.key(),"year,month,day")
      .option(TBL_NAME.key(),"user_consumption_day_aggr")
      .save("hdfs://master:8020/user/hive/warehouse/dws_ds_hudi.db/user_consumption_day_aggr")
    // 关闭SparkSession
    ss.close()
  }

}
