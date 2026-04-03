package edu.cmp26

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object OdsBaseDimensionSync {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val ss: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ODS_Layer_Ingestion_BaseDimension")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

    val mysqlUrl = "jdbc:mysql://192.168.45.100:3306/shtd_industry?useSSL=false&serverTimezone=UTC"
    val mysqlDriver = "com.mysql.jdbc.Driver"
    val mysqlUser = "root"
    val mysqlPass = "123456"

    val hudiRootPath = "hdfs://192.168.45.100:8020/user/hive/warehouse"
    val etlDate = java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"))

    println(s"=== Start Syncing ODS Base Dimensions for Date: $etlDate ===")

    // ==========================================
    // 任务 1: BaseMachine -> ods_hudi.base_machine
    // ==========================================
    val dfBaseMachine: DataFrame = ss.read.format("jdbc")
      .option("url", mysqlUrl)
      .option("driver", mysqlDriver)
      .option("user", mysqlUser)
      .option("password", mysqlPass)
      .option("dbtable", "BaseMachine")
      .load()

    val dfBaseMachineWithCol = dfBaseMachine
      .withColumn("create_time", current_timestamp())
      .withColumn("dt", lit(etlDate))

    dfBaseMachineWithCol.write
      .mode(SaveMode.Append)
      .format("hudi")
      //.options(getQuickstartWriteConfigs)
      .option(TABLE_TYPE.key, HoodieTableType.MERGE_ON_READ.name())
      .option(RECORDKEY_FIELD.key, "BaseMachineID")
      .option(PRECOMBINE_FIELD.key, "MachineAddDate")
      .option(PARTITIONPATH_FIELD.key, "dt")
      .option(TBL_NAME.key, "base_machine")
      .save(s"$hudiRootPath/ods_hudi.db/base_machine")

    println(">>> BaseMachine Synced.")

    // ==========================================
    // 任务 2: EnvironmentData -> ods_hudi.env_data_hist
    // ==========================================
    val dfEnvData: DataFrame = ss.read.format("jdbc")
      .option("url", mysqlUrl)
      .option("driver", mysqlDriver)
      .option("user", mysqlUser)
      .option("password", mysqlPass)
      .option("dbtable", "EnvironmentData")
      .load()

    val dfEnvDataWithCol = dfEnvData
      .withColumnRenamed("InPutTime","InputTime")
      .withColumn("create_time", current_timestamp())
      .withColumn("dt", lit(etlDate))

    dfEnvDataWithCol.write
      .mode(SaveMode.Append)
      .format("hudi")
      //.options(getQuickstartWriteConfigs)
      .option(TABLE_TYPE.key, HoodieTableType.MERGE_ON_READ.name())
      .option(RECORDKEY_FIELD.key, "EnvoId")
      .option(PRECOMBINE_FIELD.key, "InputTime")
      .option(PARTITIONPATH_FIELD.key, "dt")
      .option(TBL_NAME.key, "env_data_hist")
      .save(s"$hudiRootPath/ods_hudi.db/env_data_hist")

    println(">>> EnvironmentData Synced.")

    ss.close()
    println("=== All Tasks Completed ===")
  }
}
