package com.shtd.ml

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{MinMaxScaler, VectorAssembler}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.shtd.util.ConfigUtil
import ml.dmlc.xgboost4j.scala.spark.XGBoostRegressor

object DeviceRULPrediction {
  def main(args: Array[String]): Unit = {
    // 加载配置
    val configPath = "config.properties"
    val hdfsUrl = ConfigUtil.getString("hdfs.url")
    val mysqlUrl = ConfigUtil.getString("mysql.url")
    val mysqlUser = ConfigUtil.getString("mysql.username")
    val mysqlPassword = ConfigUtil.getString("mysql.password")
    //val modelSavePath = ConfigUtil.getString("xgboost.rul.model.path")

    println("=" * 80)
    println("设备剩余寿命（RUL）预测模型训练")
    println("=" * 80)
    println(s">>> HDFS: $hdfsUrl")
    println(s">>> MySQL: $mysqlUrl")
    println(s">>> 模型保存路径：$hdfsUrl/models/Device_Rul_xgboost_v1")

    // 初始化 Spark
    val spark = SparkSession.builder()
      .appName("DeviceRULPrediction")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.default.parallelism", "8")
      .getOrCreate()

    import spark.implicits._

    // 1. 读取设备数据
    println("\n>>> 正在读取设备数据...")

    val changeRecordDF = spark.read
      .format("jdbc")
      .option("url", mysqlUrl)
      .option("dbtable", "ChangeRecord")
      .option("user", mysqlUser)
      .option("password", mysqlPassword)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("fetchSize", "10000")
      .load()

    println(s">>> ChangeRecord 记录数：${changeRecordDF.count()}")

    if (changeRecordDF.count() == 0) {
      println("错误：没有读取到数据！")
      spark.stop()
      return
    }

    // 打印列名检查
    println(">>> 可用列名:")
    changeRecordDF.columns.foreach(println)

    // 2. 特征工程 - 不聚合，直接使用原始记录
    println("\n>>> 正在进行特征工程...")

    val featuresDF = changeRecordDF
      .withColumn("start_time", to_timestamp($"ChangeStartTime"))
      .withColumn("end_time", to_timestamp($"ChangeEndTime"))
      .withColumn("duration_seconds",
        (unix_timestamp($"end_time") - unix_timestamp($"start_time")).cast("double"))
      .withColumn("is_running",
        when($"ChangeRecordState" === "运行", 1.0).otherwise(0.0))
      .withColumn("is_standby",
        when($"ChangeRecordState" === "待机", 1.0).otherwise(0.0))
      .withColumn("is_offline",
        when($"ChangeRecordState" === "离线", 1.0).otherwise(0.0))
      .withColumn("is_alarm",
        when($"ChangeRecordData".contains("报警"), 1.0).otherwise(0.0))
      .withColumn("cutting_time",
        regexp_extract($"ChangeRecordData", """<col ColName="切削时间">([0-9]+)</col>""", 1).cast("double"))
      .withColumn("cycle_time",
        regexp_extract($"ChangeRecordData", """<col ColName="循环时间">([0-9]+)</col>""", 1).cast("double"))
      .withColumn("total_parts",
        regexp_extract($"ChangeRecordData", """<col ColName="总加工个数">([0-9]+)</col>""", 1).cast("double"))
      .withColumn("spindle_load",
        regexp_extract($"ChangeRecordData", """<col ColName="主轴负载">([0-9.]+)</col>""", 1).cast("double"))
      .na.fill(Map(
        "cutting_time" -> 0.0,
        "cycle_time" -> 0.0,
        "total_parts" -> 0.0,
        "spindle_load" -> 0.0
      ))
      .withColumn("record_date", to_date($"start_time"))

    println(s">>> 特征工程完成，记录数：${featuresDF.count()}")

    // 3. 构建设备级别的累计特征（使用窗口函数，但不聚合）
    println("\n>>> 正在构建累计特征...")

    val deviceWindow = Window.partitionBy("ChangeMachineID").orderBy("record_date", "ChangeStartTime")

    val featuresWithCumulative = featuresDF
      .withColumn("cumulative_runtime",
        sum("duration_seconds").over(deviceWindow.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
      .withColumn("cumulative_parts",
        sum("total_parts").over(deviceWindow.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
      .withColumn("cumulative_alarms",
        sum("is_alarm").over(deviceWindow.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
      .withColumn("avg_spindle_load_10",
        avg("spindle_load").over(deviceWindow.rowsBetween(-9, 0)))
      .withColumn("avg_cutting_time_10",
        avg("cutting_time").over(deviceWindow.rowsBetween(-9, 0)))

    println(s">>> 累计特征构建完成，记录数：${featuresWithCumulative.count()}")

    // 4. 构建标签（RUL）- 基于累计运行时间
    println("\n>>> 计算 RUL 标签...")

    println(">>> 累计运行时间统计:")
    featuresWithCumulative.select(
      avg("cumulative_runtime").as("avg_runtime"),
      max("cumulative_runtime").as("max_runtime"),
      min("cumulative_runtime").as("min_runtime"),
      avg("total_parts").as("avg_parts")
    ).show(false)

    /*val labeledDF = featuresWithCumulative
      .withColumn("design_lifetime", lit(1000000.0))
      .withColumn("RUL",
        (col("design_lifetime") - col("cumulative_runtime")) / col("design_lifetime"))
      .withColumn("RUL_normalized", col("RUL"))
      .filter(col("RUL").isNotNull)
      .filter(col("RUL") > 0)
      .filter(col("RUL") <= 1)
      .drop("design_lifetime")*/
    // 基于实际业务数据定义 RUL
    val labeledDF = featuresWithCumulative
      // 计算每台设备的最大累计运行时间（历史数据中的最大值）
      .withColumn("max_runtime_per_device",
        max("cumulative_runtime").over(Window.partitionBy("ChangeMachineID")))
      // RUL = 1 - 当前已用寿命 / 总寿命
      .withColumn("RUL",
        lit(1.0) - col("cumulative_runtime") / col("max_runtime_per_device"))
      .withColumn("RUL_normalized", col("RUL"))
      .filter(col("RUL").isNotNull)
      .filter(col("RUL") > 0)
      .filter(col("RUL") <= 1)
      .drop("max_runtime_per_device")

    println(s">>> 有效样本数：${labeledDF.count()}")

    if (labeledDF.count() == 0) {
      println("错误：没有有效样本！")
      spark.stop()
      return
    }

    println("\n>>> RUL 统计信息:")
    labeledDF.select("RUL", "RUL_normalized", "cumulative_runtime", "cumulative_parts").describe().show()

    // 5. 特征向量组装和归一化
    println("\n>>> 正在组装特征向量...")
    val featureCols = Array(
      "duration_seconds", "is_running", "is_standby", "is_offline", "is_alarm",
      "cutting_time", "cycle_time", "total_parts", "spindle_load",
      "cumulative_runtime", "cumulative_parts", "cumulative_alarms",
      "avg_spindle_load_10", "avg_cutting_time_10"
    )

    val cleanLabeledDF = labeledDF
      .filter(col("RUL").isNotNull)
      .filter(col("RUL") > 0)
      .filter(col("RUL") <= 1)

    println(s">>> 清洗后的样本数：${cleanLabeledDF.count()}")

    println(">>> 各列 null 值统计:")
    cleanLabeledDF.select(featureCols.map(c =>
      sum(when(col(c).isNull, 1).otherwise(0)).alias(s"${c}_null")
    ): _*).show(false)

    val filledDF = cleanLabeledDF.na.fill(Map(
      "duration_seconds" -> 0.0,
      "cutting_time" -> 0.0,
      "cycle_time" -> 0.0,
      "total_parts" -> 0.0,
      "spindle_load" -> 0.0,
      "cumulative_runtime" -> 0.0,
      "cumulative_parts" -> 0.0,
      "cumulative_alarms" -> 0.0,
      "avg_spindle_load_10" -> 0.0,
      "avg_cutting_time_10" -> 0.0
    ))

    val cleanDF = filledDF
      .withColumn("duration_seconds",
        when(col("duration_seconds").isin(Double.PositiveInfinity, Double.NegativeInfinity) || col("duration_seconds").isNaN, 0.0)
          .otherwise(col("duration_seconds")))
      .withColumn("cumulative_runtime",
        when(col("cumulative_runtime").isin(Double.PositiveInfinity, Double.NegativeInfinity) || col("cumulative_runtime").isNaN, 0.0)
          .otherwise(col("cumulative_runtime")))
      .withColumn("cumulative_parts",
        when(col("cumulative_parts").isin(Double.PositiveInfinity, Double.NegativeInfinity) || col("cumulative_parts").isNaN, 0.0)
          .otherwise(col("cumulative_parts")))
      .withColumn("cumulative_alarms",
        when(col("cumulative_alarms").isin(Double.PositiveInfinity, Double.NegativeInfinity) || col("cumulative_alarms").isNaN, 0.0)
          .otherwise(col("cumulative_alarms")))
      .withColumn("avg_spindle_load_10",
        when(col("avg_spindle_load_10").isin(Double.PositiveInfinity, Double.NegativeInfinity) || col("avg_spindle_load_10").isNaN, 0.0)
          .otherwise(col("avg_spindle_load_10")))
      .withColumn("avg_cutting_time_10",
        when(col("avg_cutting_time_10").isin(Double.PositiveInfinity, Double.NegativeInfinity) || col("avg_cutting_time_10").isNaN, 0.0)
          .otherwise(col("avg_cutting_time_10")))

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("features_raw")
      .setHandleInvalid("skip")

    val scaler = new MinMaxScaler()
      .setInputCol("features_raw")
      .setOutputCol("features")
      .setMin(0.0)
      .setMax(1.0)

    val assembledDF = assembler.transform(cleanDF)

    val nullFeaturesCount = assembledDF.filter(col("features_raw").isNull).count()
    println(s">>> 特征向量为 null 的记录数：$nullFeaturesCount")

    val scaledDF = scaler.fit(assembledDF).transform(assembledDF)
      .select("features", "RUL", "RUL_normalized", "ChangeMachineID")

    val validDF = scaledDF.filter(col("features").isNotNull)

    println(s">>> 有效样本数：${validDF.count()}")

    println(">>> 详细检查特征向量:")
    val featureStats = validDF.select("features").rdd.map { row =>
      val vector = row.getAs[org.apache.spark.ml.linalg.Vector](0)
      if (vector == null) {
        (1L, 0L, 0L)
      } else {
        val arr = vector.toArray
        val hasNaN = arr.exists(_.isNaN)
        val hasInf = arr.exists(_.isInfinity)
        (0L, if (hasNaN) 1L else 0L, if (hasInf) 1L else 0L)
      }
    }.reduce { (a, b) =>
      (a._1 + b._1, a._2 + b._2, a._3 + b._3)
    }

    println(s"   - Null 特征向量：${featureStats._1}")
    println(s"   - 含 NaN 特征向量：${featureStats._2}")
    println(s"   - 含 Infinity 特征向量：${featureStats._3}")

    val finalDF = validDF.filter { row =>
      val vector = row.getAs[org.apache.spark.ml.linalg.Vector](0)
      if (vector == null) false
      else {
        val arr = vector.toArray
        !arr.exists(_.isNaN) && !arr.exists(_.isInfinity)
      }
    }

    println(s">>> 最终有效样本数：${finalDF.count()}")

    if (finalDF.count() == 0) {
      println("错误：最终没有有效数据！")
      spark.stop()
      return
    }

    // 6. 划分训练集和测试集
    val Array(trainData, testData) = finalDF.randomSplit(Array(0.8, 0.2), seed = 42)

    println(s"\n>>> 训练集大小：${trainData.count()}")
    println(s">>> 测试集大小：${testData.count()}")

    if (trainData.count() == 0) {
      println("错误：训练集为空！")
      spark.stop()
      return
    }

    // 7. 配置 XGBoost 回归器
    println("\n>>> 配置 XGBoost 模型参数...")
    val xgbRegressor = new XGBoostRegressor(
      Map(
        "eta" -> "0.1",
        "max_depth" -> "6",
        "objective" -> "reg:squarederror",
        "num_round" -> "100",
        "min_child_weight" -> "5",
        "subsample" -> "0.8",
        "colsample_bytree" -> "0.8",
        "missing" -> "0.0"
      )
    ).setLabelCol("RUL_normalized")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")

    // 8. 训练模型
    println("\n" + "=" * 80)
    println("开始训练 XGBoost RUL 预测模型...")
    println("=" * 80)

    val startTime = System.currentTimeMillis()
    val model = xgbRegressor.fit(trainData)
    val trainingTime = (System.currentTimeMillis() - startTime) / 1000.0

    println(s">>> 训练完成！耗时：${trainingTime}%.2f 秒")

    // 9. 评估模型
    println("\n" + "=" * 80)
    println("模型评估")
    println("=" * 80)

    //val predictions = model.transform(testData)

    val rmseEvaluator = new RegressionEvaluator()
      .setLabelCol("RUL_normalized")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val r2Evaluator = new RegressionEvaluator()
      .setLabelCol("RUL_normalized")
      .setPredictionCol("prediction")
      .setMetricName("r2")

    /*val rmse = rmseEvaluator.evaluate(predictions)
    val r2 = r2Evaluator.evaluate(predictions)

    println(s">>> 测试集 RMSE: ${rmse}%.6f")
    println(s">>> 测试集 R²: ${r2}%.6f")*/
    // 在训练集上评估
    val trainPredictions = model.transform(trainData)
    val trainRmse = rmseEvaluator.evaluate(trainPredictions)
    val trainR2 = r2Evaluator.evaluate(trainPredictions)

    println(s">>> 训练集 RMSE: ${trainRmse}%.6f")
    println(s">>> 训练集 R²: ${trainR2}%.6f")

    // 在测试集上评估
    val predictions = model.transform(testData)
    val rmse = rmseEvaluator.evaluate(predictions)
    val r2 = r2Evaluator.evaluate(predictions)

    println(s">>> 测试集 RMSE: ${rmse}%.6f")
    println(s">>> 测试集 R²: ${r2}%.6f")

    // 过拟合诊断
    println("\n" + "=" * 80)
    println("过拟合诊断")
    println("=" * 80)

    val rmseGap = math.abs(trainRmse - rmse)
    val r2Gap = math.abs(trainR2 - r2)

    println(s">>> RMSE 差距：${rmseGap}%.6f")
    println(s">>> R² 差距：${r2Gap}%.6f")

    if (r2Gap > 0.05 || (trainR2 - r2) > 0.05) {
      println("⚠️  警告：检测到过拟合迹象！")
      println("   建议：降低树深、增加正则化、减少迭代次数")
    } else {
      println("✅ 模型泛化能力良好")
    }

    println("\n>>> 预测结果样本（前 20 条）:")
    predictions
      .select("ChangeMachineID", "RUL", "RUL_normalized", "prediction")
      .show(20, truncate = false)

    // 10. 保存模型
    println("\n" + "=" * 80)
    println("保存模型")
    println("=" * 80)

    model.write.overwrite().save(s"$hdfsUrl/models/Device_Rul_xgboost_v1")
    println(s">>> Spark 格式模型已保存到：$hdfsUrl/models/Device_Rul_xgboost_v1")
    
    // ==========================================
    // 保存为原生 C++ XGBoost .bin 文件，以供 Flink 里的 xgboost-predictor 加载
    // ==========================================
    val localBinPath = "Device_Rul_xgboost_v1.bin"
    model.nativeBooster.saveModel(localBinPath)
    
    // 上传到 HDFS
    try {
      val conf = new org.apache.hadoop.conf.Configuration()
      conf.set("fs.defaultFS", hdfsUrl)
      val fs = org.apache.hadoop.fs.FileSystem.get(conf)
      val srcPath = new org.apache.hadoop.fs.Path(localBinPath)
      val dstPath = new org.apache.hadoop.fs.Path(s"/models/Device_Rul_xgboost_v1.bin")
      // 覆盖写入
      fs.copyFromLocalFile(false, true, srcPath, dstPath)
      println(s">>> 原生 C++ 格式模型已上传到 HDFS: /models/Device_Rul_xgboost_v1.bin")
    } catch {
      case e: Exception => println(s"上传原生模型到 HDFS 失败: ${e.getMessage}")
    }

    // 11. 总结
    println("\n" + "=" * 80)
    println("训练总结")
    println("=" * 80)
    println(s">>> 模型类型：XGBoost Regressor")
    println(s">>> 训练样本数：${trainData.count()}")
    println(s">>> 测试样本数：${testData.count()}")
    println(s">>> 特征数量：${featureCols.length}")
    println(s">>> 训练时间：${trainingTime}%.2f 秒")
    println(s">>> 测试集 RMSE: ${rmse}%.6f")
    println(s">>> 测试集 R²: ${r2}%.6f")
    println("\n设备剩余寿命（RUL）预测模型训练完成！")
    println("=" * 80)

    spark.stop()
  }
}
