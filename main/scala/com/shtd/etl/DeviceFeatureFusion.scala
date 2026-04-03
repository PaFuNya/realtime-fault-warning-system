package com.shtd.etl

import com.shtd.util.ConfigUtil
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object DeviceFeatureFusion {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Device-Feature-Fusion")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate()

    // 关键：确保导入 implicits
    import spark.implicits._

    println("=" * 80)
    println("设备特征融合任务 - 整合多源数据")
    println("=" * 80)

    ConfigUtil.loadProperties()
    val hdfsUrl = ConfigUtil.getString("hdfs.url")
    val mysqlUrl = ConfigUtil.getString("mysql.url")
    val mysqlUser = ConfigUtil.getString("mysql.username")
    val mysqlPassword = ConfigUtil.getString("mysql.password")

    println(s">>> HDFS URL: $hdfsUrl")

    // 1. 读取已有的 Hudi 特征表
    println("\n>>> 读取环境传感器特征...")
    val sensorFeaturesDF: DataFrame = spark.read
      .format("hudi")
      .load(s"$hdfsUrl/user/hive/warehouse/dwd_ds_hudi.db/dwd_environment_features")

    println(s">>> 环境特征记录数：${sensorFeaturesDF.count()}")
    println("=== 环境特征数据结构 ===")
    sensorFeaturesDF.printSchema()

    // 检查实际可用的列
    println("=== 可用列名 ===")
    sensorFeaturesDF.columns.foreach(println)

    // 2. 读取设备状态数据
    println("\n>>> 读取设备状态数据...")
    val changeRecordDF: DataFrame = spark.read
      .format("jdbc")
      .option("url", mysqlUrl)
      .option("dbtable", "ChangeRecord")
      .option("user", mysqlUser)
      .option("password", mysqlPassword)
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    println(s">>> ChangeRecord 记录数：${changeRecordDF.count()}")

    // 3. 读取加工数据
    println("\n>>> 读取加工数据...")
    val produceRecordDF: DataFrame = spark.read
      .format("jdbc")
      .option("url", mysqlUrl)
      .option("dbtable", "ProduceRecord")
      .option("user", mysqlUser)
      .option("password", mysqlPassword)
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    println(s">>> ProduceRecord 记录数：${produceRecordDF.count()}")

    // 4. 融合特征
    println("\n>>> 正在融合多源特征...")

    // 4.1 处理设备状态特征
    val deviceStatusFeatures: DataFrame = changeRecordDF
      .withColumn("is_running",
        when($"ChangeRecordState" === "运行", 1.0).otherwise(0.0))
      .withColumn("is_standby",
        when($"ChangeRecordState" === "待机", 1.0).otherwise(0.0))
      .withColumn("is_offline",
        when($"ChangeRecordState" === "离线", 1.0).otherwise(0.0))
      .withColumn("is_alarm",
        when($"ChangeRecordData".contains("报警"), 1.0).otherwise(0.0))
      .groupBy($"ChangeMachineID".alias("BaseID"))
      .agg(
        avg($"is_running").alias("running_ratio"),
        avg($"is_standby").alias("standby_ratio"),
        avg($"is_offline").alias("offline_ratio"),
        avg($"is_alarm").alias("alarm_ratio"),
        count("*").alias("status_records")
      )

    // 4.2 处理加工特征
    val productionFeatures: DataFrame = produceRecordDF
      .withColumn("ProduceTotalOut_numeric",
        $"ProduceTotalOut".cast("double"))
      .withColumn("ProduceCodeCycleTime_numeric",
        $"ProduceCodeCycleTime".cast("double"))
      .groupBy($"ProduceMachineID".alias("BaseID"))
      .agg(
        sum($"ProduceTotalOut_numeric").alias("total_parts"),
        avg($"ProduceCodeCycleTime_numeric").alias("avg_cycle_time")
      )

    // 4.3 融合所有特征
    val fusedFeaturesDF: DataFrame = sensorFeaturesDF
      .join(deviceStatusFeatures, sensorFeaturesDF("BaseID") === deviceStatusFeatures("BaseID"), "left")
      .join(productionFeatures, sensorFeaturesDF("BaseID") === productionFeatures("BaseID"), "left")
      .drop(deviceStatusFeatures("BaseID"))
      .drop(productionFeatures("BaseID"))
      // 填充缺失值
      .na.fill(Map(
        "running_ratio" -> 0.0,
        "standby_ratio" -> 0.0,
        "offline_ratio" -> 0.0,
        "alarm_ratio" -> 0.0,
        "total_parts" -> 0.0,
        "avg_cycle_time" -> 0.0
      ))
      // 添加 etl_date 分区字段
      .withColumn("etl_date", date_format(current_timestamp(), "yyyy-MM-dd"))
      // 如果没有 window_end，使用当前时间作为替代
      .withColumn("window_end_temp",
        when($"window_end".isNotNull, $"window_end")
          .otherwise(to_timestamp(current_timestamp())))
      // 计算 RUL 标签
      .withColumn("RUL",
        lit(10000.0) - coalesce($"total_parts", lit(0.0)) * 0.01)
      .withColumn("health_score",
        when($"RUL" > 5000, 1.0)
          .when($"RUL" > 2000, 0.7)
          .otherwise(0.3))

    println(s">>> 融合后记录数：${fusedFeaturesDF.count()}")

    // 5. 保存融合后的特征
    println("\n>>> 保存融合特征到 Hudi...")

    // 动态检查必需字段
    val hasWindowEnd = fusedFeaturesDF.columns.contains("window_end")
    val hasWindowEndTemp = fusedFeaturesDF.columns.contains("window_end_temp")

    val finalDF: DataFrame = if (hasWindowEnd) {
      fusedFeaturesDF
    } else if (hasWindowEndTemp) {
      fusedFeaturesDF.withColumnRenamed("window_end_temp", "window_end")
    } else {
      println("警告：既没有 window_end 也没有 window_end_temp，使用当前时间")
      fusedFeaturesDF.withColumn("window_end", to_timestamp(current_timestamp()))
    }

    // 检查必需字段是否存在
    val requiredFields = Seq("BaseID", "window_end", "etl_date")
    val missingFields = requiredFields.filterNot(fieldName =>
      finalDF.columns.contains(fieldName))

    if (missingFields.nonEmpty) {
      println(s"错误：缺少必需字段：${missingFields.mkString(", ")}")
      spark.stop()
      return
    }

    println(">>> 开始写入 Hudi...")
    finalDF.write
      .mode(SaveMode.Overwrite)
      .format("hudi")
      .option("hoodie.table.name", "dwd_device_features")
      .option("hoodie.datasource.write.recordkey.field", "BaseID,window_end")
      .option("hoodie.datasource.write.precombine.field", "window_end")
      .option("hoodie.datasource.write.partitionpath.field", "etl_date")
      .option("hoodie.datasource.write.keygenerator.class",
        "org.apache.hudi.keygen.ComplexKeyGenerator")
      .option("hoodie.metadata.enable", "false")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .save(s"$hdfsUrl/user/hive/warehouse/dwd_ds_hudi.db/dwd_device_features")

    println(s">>> 融合特征已保存到：$hdfsUrl/user/hive/warehouse/dwd_ds_hudi.db/dwd_device_features")

    println("\n>>> 融合特征样本:")
    finalDF.select(
      $"BaseID", $"window_end", $"etl_date",
      $"avg_temp", $"var_temp", $"kurtosis_temp", $"fft_peak",
      $"running_ratio", $"alarm_ratio", $"total_parts", $"avg_cycle_time",
      $"RUL", $"health_score"
    ).show(10, truncate = false)

    println("\n" + "=" * 80)
    println("特征融合完成")
    println("=" * 80)
    println(s">>> 输入特征表：dwd_environment_features")
    println(s">>> 输出特征表：dwd_device_features")
    println(s">>> 融合的特征:")
    println("   - 环境传感器特征 (avg_temp, var_temp, kurtosis_temp, fft_peak)")
    println("   - 设备状态特征 (running_ratio, alarm_ratio)")
    println("   - 加工特征 (total_parts, avg_cycle_time)")
    println("   - 寿命标签 (RUL, health_score)")
    println(s">>> 可用于训练：XGBoost 故障概率模型 + XGBoost RUL 预测模型")
    println("=" * 80)

    spark.stop()
  }
}
