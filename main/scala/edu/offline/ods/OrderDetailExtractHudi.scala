package edu.offline.ods

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, date_format, lit}

object OrderDetailExtractHudi {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    // 创建sparkSession，配置运行环境
    val ss: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Hudi extract order_detail")
      .enableHiveSupport()
      .getOrCreate()


    // 创建spark读取mysql相关配置
    val readMySQLDF: DataFrame = ss.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.91.101:3306/ds_pub?characterEncoding=utf8&useSSL=false")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "order_detail")
      .load()
    //全量，只处理分区字段
    val readMySQLDFwithCol:DataFrame={
      readMySQLDF
        .withColumn("etl_date",lit("20240413"))
    }
    readMySQLDFwithCol.write
      .mode(SaveMode.Overwrite)
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(),"create_time") //预聚合主键
      .option(RECORDKEY_FIELD.key(),"id") //主键（primaryKey）
      .option(PARTITIONPATH_FIELD.key(),"etl_date") //分区字段
      .option(TBL_NAME.key(),"order_detail") //表名
      .save("hdfs://master:8020/user/hive/warehouse/ods_ds_hudi.db/order_detail") //文件路径

    // 关闭SparkSession
    ss.close()
  }

}
