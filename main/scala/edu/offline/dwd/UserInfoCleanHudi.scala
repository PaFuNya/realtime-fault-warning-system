package edu.offline.dwd

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object UserInfoCleanHudi {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    // 1、创建sparkSession，配置运行环境
    val ss: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Hudi read orderInfo")
      .enableHiveSupport()
      .getOrCreate()

    // 从ods_ds_hudi库中读取user_info表
    val odsUserInfoDF = ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/ods_ds_hudi.db/user_info")
      .filter(col("etl_date")==="20240413")// 过滤昨天的分区
      .drop("etl_date")
      .withColumn("dwd_insert_user",lit("user1"))
      .withColumn("dwd_insert_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("dwd_modify_user",lit("user1"))
      .withColumn("dwd_modify_time",lit(date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss").cast("timestamp")))
      .withColumn("etl_date",lit("20240413"))
    // 从dwd_ds_hudi库中读取dim_user_info分区表
    val dwdUserInfoDF = ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info")

   /* val latestPartitionVal = dwdUserInfoDF
      .select("etl_date") // 选择分区字段
      .distinct() // 去重
      .orderBy(col("etl_date").desc) // 根据分区字段降序排序
      .limit(1) // 取最上面的一行，即最新的分区
      .collect()(0)(0) // 提取分区值*/

    // 读取dwd最新的分区数据
    val latestPartitionDF = dwdUserInfoDF.filter(col("etl_date") === lit("20201107"))
   // latestPartitionDF.show()
    //增量清洗，需要union
    odsUserInfoDF.union(latestPartitionDF).createTempView("v")

    //operate_time倒序，留最新的，dwd_insert_time取最小值，分区为ods里面的
    ss.sql(
      """
        |select *,
        |row_number() over (partition by v.id order by operate_time desc) as operate_time_num,
        |min(dwd_insert_time) over (partition by v.id) as min_dwd_insert_time from v
        |""".stripMargin
    )
      .withColumn("dwd_insert_time",lit(col("min_dwd_insert_time")).cast("timestamp"))
      .filter(col("operate_time_num")===1)
      .drop("operate_time_num","min_dwd_insert_time","etl_date")
      .withColumn("create_time",
        when(length(col("create_time"))===10,
          concat(col("create_time"),lit(" 00:00:00"))
        ).otherwise(col("create_time"))
      )
      .withColumn("operate_time",
        when(length(col("operate_time"))===10,
          concat(col("operate_time"),lit(" 00:00:00"))
        ).otherwise(col("operate_time"))
      )
      .withColumn("etl_date",lit("20240413"))//值与ods_ds_hudi对应表该值相等
      .createTempView("result_v")
    val resultDF=ss.sql(
      """
        |select * from result_v
        |""".stripMargin
    )
    //resultDF.show()
    //3、写入hudi，配置Hudi的写入选项，包括主键、预聚合字段、分区等。
    resultDF.write
      .mode(SaveMode.Append)
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(),"operate_time") //预聚合主键
      .option(RECORDKEY_FIELD.key(),"id") //主键（primaryKey）
      .option(PARTITIONPATH_FIELD.key(),"etl_date") //分区字段
      .option(TBL_NAME.key(),"dim_user_info") //表名
      .save("hdfs://master:8020/user/hive/warehouse/dwd_ds_hudi.db/dim_user_info") //文件路径*/
  }
}
