package edu.offline.dwd

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, concat, current_timestamp, date_format, length, lit, when}

object BaseProvinceCleanHudi {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    // 1、创建sparkSession，配置运行环境
    val ss: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Hudi clean baseProvince")
      .enableHiveSupport()
      .getOrCreate()

    //    import ss.implicits._
    //2、从ods中读取base_province表的数据:过滤分区、修改分区字段，添加列、处理时间戳字段
    val odsUserInfo=ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/ods_ds_hudi.db/base_province")
      .filter(col("etl_date")==="20240413")
      .drop("etl_date")
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_insert_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_modify_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("etl_date",lit("20240413"))
    odsUserInfo.show()

    //原库中无dwd_ds_hudi.db/dim_province,不进行union，直接插入
    //3、写入hudi，配置Hudi的写入选项，包括主键、预聚合字段、分区等。
    odsUserInfo.write
      .mode(SaveMode.Overwrite)
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(),"dwd_modify_time") //预聚合主键
      .option(RECORDKEY_FIELD.key(),"id") //主键（primaryKey）
      .option(PARTITIONPATH_FIELD.key(),"etl_date") //分区字段
      .option(TBL_NAME.key(),"dim_province") //表名
      .save("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/dim_province") //文件路径

    // 关闭SparkSession
    ss.close()
  }

}
