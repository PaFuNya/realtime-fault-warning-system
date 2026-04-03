package edu.offline.dwd

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, concat, current_timestamp, date_format, length, lit, when}

object SkuInfoCleanHudi {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    // 1、创建sparkSession，配置运行环境
   val ss:SparkSession=SparkSession.builder()
     .master("local[*]")
     .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
     .appName("Hudi clean skuInfo")
     .enableHiveSupport()
     .getOrCreate()

    // 从ods_ds_hudi库中读取sku_info表
   val odsSkuInfoDF=ss.read.format("hudi")
     .load("hdfs://192.168.45.11:9000/user/hive/warehouse/ods_ds_hudi.db/sku_info")
     .filter(col("etl_date")==="20240413")
     .drop("etl_date")
     .withColumn("dwd_insert_user",lit("user1"))
     .withColumn("dwd_insert_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))
     .withColumn("dwd_modify_user",lit("user1"))
     .withColumn("dwd_modify_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))
     .withColumn("etl_date",lit("20240413"))
    odsSkuInfoDF.show()
    // 从dwd_ds_hudi库中读取dim_sku_info分区表(最新分区)
    val dwdSkuInfoDF=ss.read.format("hudi")
      .load("hdfs://192.168.45.11:9000/user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info")
      .filter(col("etl_date")==="20201107")
    dwdSkuInfoDF.show()
    //增量清洗，需要union
    odsSkuInfoDF.union(dwdSkuInfoDF).createTempView("v")

    //operate_time倒序，留最新的，dwd_insert_time取最小值，分区为ods里面的
    ss.sql(
      """
        |select *,
        |row_number() over (partition by v.id order by create_time desc) as rowNumber,
        |min(dwd_insert_time) over (partition by v.id) as min_insert_time
        |from v
        |""".stripMargin
    )
      .withColumn("dwd_insert_time",lit(col("min_insert_time")).cast("timestamp"))
      .filter(col("rowNumber")===1)
      .drop("rowNumber","min_insert_time","etl_date")
      .withColumn("create_time",
        when(length(col("create_time"))===10,
          concat(col("create_time"),lit(" 00:00:00"))
        ).otherwise(col("create_time"))
      )
      .withColumn("etl_date",lit("20240413"))
      .createTempView("result_v")
    val resultDF=ss.sql(
      """
        |select * from result_v
        |""".stripMargin
    )
    resultDF.show()
    //3、写入hudi，配置Hudi的写入选项，包括主键、预聚合字段、分区等。
    resultDF.write
      .mode(SaveMode.Append)
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(),"dwd_modify_time") //预聚合主键
      .option(RECORDKEY_FIELD.key(),"id") //主键（primaryKey）
      .option(PARTITIONPATH_FIELD.key(),"etl_date") //分区字段
      .option(TBL_NAME.key(),"dim_sku_info") //表名
      .save("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/dim_sku_info") //文件路径*/
  }
}
