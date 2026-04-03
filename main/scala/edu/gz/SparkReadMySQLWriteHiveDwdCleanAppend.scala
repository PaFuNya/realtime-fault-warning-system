package edu.gz

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, date_format, lit}

object SparkReadMySQLWriteHiveDwdCleanAppend {
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

    /*val ods_order_info:DataFrame=ss.sql("select id,user_id,create_time,operate_time from ods.order_info limit 10")
      .withColumn("dwd_insert_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("dwd_modify_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("etl_date",lit(date_format(current_date(),"yyyyMMdd")))

    val dwd_order_info:DataFrame=ss.sql("select id,user_id,create_time,operate_time,dwd_insert_time,dwd_modify_time,etl_date from dwd.order_info limit 10")

    ods_order_info.union(dwd_order_info).createTempView("v")

    ss.sql(
        """
          |select *,
          | row_number() over (partition by v.id order by operate_time desc) as insert_time_num,
          | min(dwd_insert_time) over (partition by v.id) as min_dwd_insert_time from v
          |""".stripMargin) //.show()
      .withColumn("dwd_insert_time", lit(col("min_dwd_insert_time")).cast("timestamp"))
      .filter(col("insert_time_num")===1)
      .drop("insert_time_num","min_dwd_insert_time")
      .withColumn("etl_date",lit(date_format(current_date(),"yyyyMMdd")))
      .createTempView("v_result")
    ss.sql(
      """
        |select * from v_result
        |""".stripMargin
    ).show()*/

    val ods_order_info:DataFrame=ss.sql("select * from ods.order_info")
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_insert_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_modify_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("etl_date",lit(date_format(current_date(),"yyyyMMdd")))

    val dwd_order_info:DataFrame=ss.sql("select * from dwd.order_info")

    ods_order_info.union(dwd_order_info).createTempView("v")

    ss.sql(
        """
          |select *,
          | row_number() over (partition by v.id order by operate_time desc) as insert_time_num,
          | min(dwd_insert_time) over (partition by v.id) as min_dwd_insert_time from v
          |""".stripMargin) //.show()
      .withColumn("dwd_insert_time", lit(col("min_dwd_insert_time")).cast("timestamp"))
      .filter(col("insert_time_num")===1)
      .drop("insert_time_num","min_dwd_insert_time")
      .withColumn("etl_date",lit(date_format(current_date(),"yyyyMMdd")))
      .createTempView("v_result")
    ss.sql(
      """
        |insert overwrite table dwd.order_info ( select * from v_result )
        |""".stripMargin
    )
    ss.sql("select * from dwd.order_info limit 2").show()


    ss.close()
  }
}
