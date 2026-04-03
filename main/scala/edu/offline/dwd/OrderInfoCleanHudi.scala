package edu.offline.dwd

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.sql.functions.{col, current_date, current_timestamp, date_format, from_unixtime, lit, to_timestamp, unix_timestamp, when}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions._

object OrderInfoCleanHudi {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","root")
    // 1、创建sparkSession，配置运行环境
    val ss: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Hudi clean orderInfo")
      .enableHiveSupport()
      .getOrCreate()

//    import ss.implicits._
    //2、从ods中读取order_info_par表的数据:过滤分区、修改分区字段，添加列、处理时间戳字段
    val odsUserInfo=ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/ods_ds_hudi.db/order_info_par")
      .filter(col("etl_date")==="20240413")
      .drop("etl_date")
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_insert_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_modify_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("create_time",
        when(length(col("create_time")) === 10,
          concat(col("create_time"), lit(" 00:00:00"))
        ).otherwise(col("create_time"))
      )
      .withColumn("operate_time",
        when(length(col("operate_time")) === 10,
          concat(col("operate_time"), lit(" 00:00:00"))
        ).otherwise(col("operate_time"))
      )
      .withColumn("etl_date",lit(date_format(col("create_time"),"yyyyMMdd")))
    odsUserInfo.show()

    //原库中无dwd_ds_hudi.db/fact_order_info,不进行union，直接插入
    //3、写入hudi，配置Hudi的写入选项，包括主键、预聚合字段、分区等。
    odsUserInfo.write
      .mode(SaveMode.Overwrite)
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(),"operate_time") //预聚合主键
      .option(RECORDKEY_FIELD.key(),"id") //主键（primaryKey）
      .option(PARTITIONPATH_FIELD.key(),"etl_date") //分区字段
      .option(TBL_NAME.key(),"fact_order_info") //表名
      .save("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/fact_order_info") //文件路径

    // 关闭SparkSession
    ss.close()
  }

}
