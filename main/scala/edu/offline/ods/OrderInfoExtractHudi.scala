package edu.offline.ods

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_date, date_format, greatest, lit, when}

import java.sql.Timestamp

object OrderInfoExtractHudi {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    // 创建sparkSession，配置运行环境
    val ss: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Hudi extract OrderInfo")
      .enableHiveSupport()
      .getOrCreate()

    //增量抽取
    // 创建spark读取mysql相关配置，添加增量字段
    val readMySQLDF: DataFrame = ss.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.91.101:3306/ds_pub?characterEncoding=utf8&useSSL=false")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_info")
      .load()
      .withColumn("increment_field",lit(greatest(col("operate_time"),col("create_time"))))

    //读取ods中原有数据,并增加一列(较大值)，排序取最大值列
    val latestRecord=ss.read.format("hudi")
      .load("hdfs://master:8020/user/hive/warehouse/ods_ds_hudi.db/order_info_par")
      .withColumn("gTime",greatest(col("operate_time"),col("create_time")))
      .orderBy(col("gTime").desc)
      .limit(1)
      .collect()
    //取出不为空的值，为空则取“1970-01-01 00:00:00”
    val latest=if(!latestRecord.isEmpty)
      latestRecord(0).getAs[Timestamp]("gTime")
    else
      "1970-01-01 00:00:00"
    //print(latest)

    //增量字段与ods中的最大字段进行比较,过滤出需要增量的数据，
    //  删除df中的增量字段，处理operate_time字段，增加分区字段
    val readMySQLDFwithCol:DataFrame={
      readMySQLDF
        .filter(col("increment_field")>latest)
        .drop("increment_field")
        .withColumn("operate_time",when(col("operate_time").isNull,col("create_time")).otherwise(col("operate_time")))
        .withColumn("etl_date",lit("20240413"))
    }
    //需要导入这三个包，打import org.apache.hudi.有提示
    import org.apache.hudi.QuickstartUtils._
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._


    /*readMySQLDFwithCol.write
      .mode(SaveMode.Overwrite)
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(),"operate_time") //预聚合主键
      .option(RECORDKEY_FIELD.key(),"id") //主键（primaryKey）
      .option(PARTITIONPATH_FIELD.key(),"etl_date") //分区字段
      .option(TBL_NAME.key(),"order_info") //表名
      .save("hdfs://master:8020/user/hive/warehouse/ods_ds_hudi.db/user_info") //文件路径*/

    readMySQLDFwithCol.write
      .mode(SaveMode.Append)
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(),"operate_time") //预聚合主键
      .option(RECORDKEY_FIELD.key(),"id") //主键（primaryKey）
      .option(PARTITIONPATH_FIELD.key(),"etl_date") //分区字段
      .option(TBL_NAME.key(),"order_info_par") //表名
      .save("hdfs://master:8020/user/hive/warehouse/ods_ds_hudi.db/order_info_par") //文件路径

    // 关闭SparkSession
    ss.close()
  }

}
