package com.shtd.ml

import com.shtd.util.ConfigUtil
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FaultProbabilityModel {
  def main(args: Array[String]): Unit = {
    // 加载配置文件（自动从 classpath 加载 config.properties）
    ConfigUtil.loadProperties()
    val hdfsUrl=ConfigUtil.getString("hdfs.url")
    val mysqlUrl=ConfigUtil.getString("mysql.url")

    val spark = SparkSession.builder()
      .appName("XGBoost-Fault-Probability")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.default.parallelism", "8")
      .getOrCreate()

    import spark.implicits._

    // 1. 读取 Hudi 特征数据
    val featureDF = spark.read
      .format("hudi")
      .load(s"$hdfsUrl/user/hive/warehouse/dwd_ds_hudi.db/dwd_environment_features")
      .filter($"etl_date" >= "2026-03-01") // 示例：只取最近的数据训练

    println(s">>> 读取到的数据行数：${featureDF.count()}")

    if (featureDF.count() == 0) {
      println("错误：没有读取到数据！")
      println(">>> 尝试读取所有分区...")

      val featureDFAll = spark.read
        .format("hudi")
        .load(s"$hdfsUrl/user/hive/warehouse/dwd_ds_hudi.db/dwd_environment_features")

      println(s">>> 总数据行数：${featureDFAll.count()}")
      println("=== 数据样本 ===")
      featureDFAll.show(10, truncate = false)

      if (featureDFAll.count() == 0) {
        println("错误：Hudi 表完全是空的！")
        spark.stop()
        return
      }

      // 使用全部数据
      val featureDF = featureDFAll
    }

    println("=== 特征数据样本 ===")
    featureDF.show(10, truncate = false)

    println("=== 分区信息 ===")
    featureDF.select("etl_date").distinct().show(false)
    // 2. 生成标签 (Label Generation)
    // 业务规则：如果 平均温度 > 80 或 FFT峰值 > 1000 (假设值)，标记为故障 (1.0)
    //val labeledDF = featureDF
    //  .withColumn("label", when($"avg_temp" > 80.0 || $"fft_peak" > 1000.0, 1.0).otherwise(0.0))

    // 方案 B: 基于统计的动态阈值（推荐）
    val stats = featureDF.agg(
      avg("avg_temp").as("mean_temp"),
      stddev("avg_temp").as("std_temp"),
      avg("fft_peak").as("mean_fft"),
      stddev("fft_peak").as("std_fft")
    ).first()

    val meanTemp = stats.getDouble(0)
    val stdTemp = stats.getDouble(1)
    val meanFft = stats.getDouble(2)
    val stdFft = stats.getDouble(3)

    // 使用 1.5-sigma 原则确保有足够的正样本
    val tempThreshold = meanTemp + 1.5 * stdTemp
    val fftThreshold = meanFft + 1.5 * stdFft

    println(s">>> 温度阈值：$tempThreshold (均值=$meanTemp, 标准差=$stdTemp)")
    println(s">>> FFT 峰值阈值：$fftThreshold (均值=$meanFft, 标准差=$stdFft)")
    var labeledDF = featureDF
      .withColumn("label",
        when($"avg_temp" > tempThreshold || $"fft_peak" > fftThreshold, 1.0)
          .otherwise(0.0))

    println("=== 初始标签分布 ===")
    labeledDF.groupBy("label").count().show()
    // 如果正样本太少，调整阈值
    val positiveCount = labeledDF.filter($"label" === 1.0).count()
    val totalCount = labeledDF.count()
    val positiveRatio = positiveCount.toDouble / totalCount

    if (positiveRatio < 0.01) {
      println(s">>> 警告：正样本比例过低 ($positiveRatio)，正在调整阈值...")
      // 降低阈值到 2 个标准差
      val newTempThreshold = meanTemp + 2 * stdTemp
      val newFftThreshold = meanFft + 2 * stdFft

      println(s">>> 新的温度阈值：$newTempThreshold")
      println(s">>> 新的 FFT 阈值：$newFftThreshold")

      var labeledDF = featureDF
        .withColumn("label",
          when($"avg_temp" > newTempThreshold || $"fft_peak" > newFftThreshold, 1.0)
            .otherwise(0.0))

      println("=== 调整后的标签分布 ===")
      labeledDF.groupBy("label").count().show()

      // 如果还是太少，再降低到 1.5 个标准差
      val newPositiveRatio = labeledDF.filter($"label" === 1.0).count().toDouble / labeledDF.count()
      if (newPositiveRatio < 0.01) {
        println(s">>> 正样本仍然过少 ($newPositiveRatio)，继续调整...")
        val finalTempThreshold = meanTemp + 1.5 * stdTemp
        val finalFftThreshold = meanFft + 1.5 * stdFft

        labeledDF = featureDF
          .withColumn("label",
            when($"avg_temp" > finalTempThreshold || $"fft_peak" > finalFftThreshold, 1.0)
              .otherwise(0.0))

        println("=== 最终标签分布 ===")
        labeledDF.groupBy("label").count().show()
      }
    }
    // 3. 特征向量组装
    val assembler = new VectorAssembler()
      .setInputCols(Array("avg_temp", "var_temp", "kurtosis_temp", "fft_peak"))
      .setOutputCol("features")
      .setHandleInvalid("skip")

    val assembledDF = assembler.transform(labeledDF)
      .select("features", "label") // 只保留需要的列

    // 4. 划分训练集和测试集（关键：使用分层抽样）
    val positiveData = assembledDF.filter($"label" === 1.0)
    val negativeData = assembledDF.filter($"label" === 0.0)
    println(s">>> 正样本总数：${positiveData.count()}, 负样本总数：${negativeData.count()}")

    if (positiveData.count() == 0) {
      println("错误：没有正样本！需要调整阈值。")
      spark.stop()
      return
    }

    // 分别从正负样本中按比例抽取
    val Array(posTrain, posTest) = positiveData.randomSplit(Array(0.8, 0.2), seed = 42)
    val Array(negTrain, negTest) = negativeData.randomSplit(Array(0.8, 0.2), seed = 42)

    // 合并
    val trainData = posTrain.union(negTrain)
    val testData = posTest.union(negTest)
    // 【新增】从训练集中再分出验证集，用于检测过拟合
    val Array(trainFinal, validationData) = trainData.randomSplit(Array(0.85, 0.15), seed = 42)
    println(s">>> 最终训练集：${trainFinal.count()}")
    println(s">>> 验证集：${validationData.count()}")
    println(s">>> 测试集：${testData.count()}")

    println("\n>>> 各数据集标签分布:")
    println("训练集:")
    trainFinal.groupBy("label").count().show()
    println("验证集:")
    validationData.groupBy("label").count().show()
    println("测试集:")
    testData.groupBy("label").count().show()
    //val Array(trainData, testData) = assembledDF.randomSplit(Array(0.8, 0.2), seed = 42)

    // 5. 配置 XGBoost 分类器
    val xgb = new XGBoostClassifier(
      Map(
        "eta" -> "0.1",           // 学习率
        "max_depth" -> "4",       // 树深
        "objective" -> "binary:logistic", // 二分类目标
        "num_round" -> "50",     // 迭代次数
        //"num_workers" -> "2",      // 分布式 Worker 数量
        // 关键：添加处理不平衡数据的参数
        "scale_pos_weight" -> (labeledDF.filter($"label" === 0.0).count().toDouble /
          labeledDF.filter($"label" === 1.0).count()).toString,
        "min_child_weight" -> "5",
        "subsample" -> "0.6",
        "colsample_bytree" -> "0.8"
      )
    ).setLabelCol("label")
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .setProbabilityCol("probability") // 输出概率列
      //.setNumClass(2) // 明确指定二分类
    // 6. 训练模型
    println(">>> 开始训练故障概率模型...")
    println(s">>> 训练集大小：${trainData.count()}")
    println(s">>> 测试集大小：${testData.count()}")
    println(s">>> 正负样本比例：1:${labeledDF.filter($"label" === 0.0).count().toDouble / labeledDF.filter($"label" === 1.0).count()}")
    val model = xgb.fit(trainFinal) // 注意：这里只用 trainFinal 训练
    //val model = xgb.fit(trainData)
    // 【新增】在验证集上评估，检测过拟合
    println("\n=== 过拟合诊断 ===")

    // 训练集表现
    val trainPredictions = model.transform(trainFinal)
    val trainAccuracy = trainPredictions.filter($"prediction" === $"label").count().toDouble / trainFinal.count()
    println(s">>> 训练集准确率：$trainAccuracy")

    // 验证集表现
    val valPredictions = model.transform(validationData)
    val valAccuracy = valPredictions.filter($"prediction" === $"label").count().toDouble / validationData.count()
    println(s">>> 验证集准确率：$valAccuracy")

    // 计算差距
    val gap = trainAccuracy - valAccuracy
    println(s">>> 性能差距（训练 - 验证）: $gap")

    if (gap > 0.05) {
      println(s"⚠️  警告：检测到过拟合迹象（差距=${gap}%.2f > 5%）")
      println("建议：降低树深、增加正则化、减少迭代次数")
    } else if (gap > 0.02) {
      println(s"ℹ️  轻微过拟合（差距=${gap}%.2f），可以接受")
    } else {
      println(s"✓ 模型状态良好（差距=${gap}%.2f）")
    }
    // 6-2. 使用全部训练数据重新训练最终模型
    println("\n>>> 使用全部训练数据重新训练最终模型...")
    val finalModel = xgb.fit(trainData)

    // 7. 评估模型
    val predictions = finalModel.transform(testData)
    // 调试：查看完整的预测结果
    println("=== 预测结果详情 ===")
    import org.apache.spark.ml.linalg.Vector
    predictions.select("label", "prediction", "probability").rdd.take(100).foreach { row =>
      val label = row.getDouble(0)
      val pred = row.getDouble(1)
    val probVector = row.getAs[Vector](2) // 正确获取 Vector 类型
    println(s"label=$label, prediction=$pred, probability=${probVector.toArray.mkString("[", ",", "]")}, size=${probVector.size}")
  }

    // 分别查看正负样本的预测情况
    println("\n=== 正样本 (label=1.0) 的预测情况 ===")
    predictions.filter($"label" === 1.0)
      .select("label", "prediction", "probability")
      .show(20, truncate = false)

    println("\n=== 负样本 (label=0.0) 的预测情况 ===")
    predictions.filter($"label" === 0.0)
      .select("label", "prediction", "probability")
      .show(20, truncate = false)

    // 调试：查看预测结果的结构
    //println("=== 预测结果样本 ===")
    //predictions.select("label", "prediction", "probability").show(20, truncate = false)

    // 检查数据分布
    println("=== 标签分布 ===")
    predictions.groupBy("label").count().show()
    println("=== 预测分布 ===")
    predictions.groupBy("prediction").count().show()
    // 计算混淆矩阵
    val tp = predictions.filter($"label" === 1.0 && $"prediction" === 1.0).count()
    val tn = predictions.filter($"label" === 0.0 && $"prediction" === 0.0).count()
    val fp = predictions.filter($"label" === 0.0 && $"prediction" === 1.0).count()
    val fn = predictions.filter($"label" === 1.0 && $"prediction" === 0.0).count()

    println(s">>> 混淆矩阵：TP=$tp, TN=$tn, FP=$fp, FN=$fn")

    // 过滤掉可能的异常数据
    val validPredictions = predictions
      .filter($"label".isNotNull)
      .filter($"prediction".isNotNull)

    if (validPredictions.count() == 0) {
      println("错误：没有有效的预测数据！")
      spark.stop()
      return
    }
    // 使用 prediction 列计算准确率
    val accuracy = predictions.filter($"prediction" === $"label").count().toDouble / predictions.count()
    println(s">>> 测试集准确率：$accuracy")

    // 计算召回率和精确率
    val precision = if (tp + fp > 0) tp.toDouble / (tp + fp) else 0.0
    val recall = if (tp + fn > 0) tp.toDouble / (tp + fn) else 0.0
    val f1 = if (precision + recall > 0) 2 * precision * recall / (precision + recall) else 0.0

    println(s">>> 精确率：$precision, 召回率：$recall, F1 分数：$f1")

    // 尝试计算 AUC（如果失败则跳过）
    try {
      val evaluator = new BinaryClassificationEvaluator()
        .setLabelCol("label")
        .setRawPredictionCol("prediction")

      val auc = evaluator.evaluate(validPredictions)
      println(s">>> 测试集 AUC: $auc")
    } catch {
      case e: Exception =>
        println(s">>> AUC 计算失败：${e.getMessage}")
        println(">>> 继续使用准确率作为评估指标")
    }

    // 8. 保存模型
    finalModel.write.overwrite().save(s"$hdfsUrl/models/fault_probability_xgboost_v2")
    
    // ==========================================
    // 保存为原生 C++ XGBoost .bin 文件，以供 Flink 里的 xgboost-predictor 加载
    // ==========================================
    val localBinPath = "fault_probability_xgboost_v2.bin"
    finalModel.nativeBooster.saveModel(localBinPath)
    
    // 上传到 HDFS
    try {
      val conf = new org.apache.hadoop.conf.Configuration()
      conf.set("fs.defaultFS", hdfsUrl)
      val fs = org.apache.hadoop.fs.FileSystem.get(conf)
      val srcPath = new org.apache.hadoop.fs.Path(localBinPath)
      val dstPath = new org.apache.hadoop.fs.Path(s"/models/fault_probability_xgboost_v2.bin")
      // 覆盖写入
      fs.copyFromLocalFile(false, true, srcPath, dstPath)
      println(s">>> 原生 C++ 格式故障模型已上传到 HDFS: /models/fault_probability_xgboost_v2.bin")
    } catch {
      case e: Exception => println(s"上传故障原生模型到 HDFS 失败: ${e.getMessage}")
    }

    spark.stop()
  }
}