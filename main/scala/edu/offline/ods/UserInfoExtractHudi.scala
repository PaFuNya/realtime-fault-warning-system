package edu.offline.ods

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_NAME}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}

object UserInfoExtractHudi {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    // 创建sparkSession，配置运行环境
    val ss: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("Hudi extract userInfo")
      .enableHiveSupport()
      .getOrCreate()


    // 创建spark读取mysql相关配置
    val readMySQLDF: DataFrame = ss.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.45.11:3306/ds_pub?characterEncoding=utf8&useSSL=false")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .load()
    //全量，只处理一个字段和分区字段
    val readMySQLDFwithCol:DataFrame={
      readMySQLDF
        .withColumn("operate_time",when(col("operate_time").isNull,col("create_time")).otherwise(col("operate_time")))
        .withColumn("etl_date",lit("20240413"))
    }
    //需要导入这三个包，打import org.apache.hudi.有提示
    import org.apache.hudi.QuickstartUtils._
    import org.apache.hudi.DataSourceWriteOptions._
    import org.apache.hudi.config.HoodieWriteConfig._


    readMySQLDFwithCol.write
      .mode(SaveMode.Overwrite)
      .format("hudi")
      .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(),"operate_time") //预聚合主键
      .option(RECORDKEY_FIELD.key(),"id") //主键（primaryKey）
      .option(PARTITIONPATH_FIELD.key(),"etl_date") //分区字段
      .option(TBL_NAME.key(),"user_info") //表名
      .save("hdfs://192.168.45.11:9000/user/hive/warehouse/ods_ds_hudi.db/user_info") //文件路径

    // 关闭SparkSession
    ss.close()
  }
}
