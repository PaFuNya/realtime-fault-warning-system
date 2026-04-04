package org.example.tasks

import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{HashingTF, IDF, RegexTokenizer}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
import java.io.File

/** 任务组 5：实时指标与推理 (Spark -> ClickHouse)
  *   16. 实时异常检测与报警 源：Kafka sensor_raw + log_raw (经过合并) 目标：ClickHouse
  *       shtd_ads.realtime_alerts & Kafka alert_topic (2.4要求) 规则引擎：温度 > 阈值 OR
  *       振动突增 > 20% OR Error Code 999 突增。
  */
object Task16_RealtimeAlerts {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession("Task16_RealtimeAlerts")

    // 1. 读取 Sensor 流
    val sensorRawDF = SparkUtils.getSensorRawStream(spark)

    // 2. 读取 Log 流
    val logRawDF = SparkUtils.getLogRawStream(spark)

    // 3. 来自传感器流的异常 (温度 > 80.0 或 振动异常 这里用基础值模拟)
    val sensorAlerts = sensorRawDF
      .filter(col("temperature") > 80.0 || col("vibration_x") > 2.0)
      .select(
        col("timestamp").alias("alert_time"),
        col("machine_id"),
        when(col("temperature") > 80.0, "High Temperature")
          .otherwise("Vibration Sudden Increase > 20%")
          .alias("alert_type"),
        col("temperature").alias("trigger_value"),
        lit(80.0).alias("threshold_value"),
        lit("Inspect machine immediately").alias("suggested_action")
      )

    // 4. 日志异常检测 (结合已知规则和机器学习聚类)
    // 规则 1：匹配已知的 Error Code 999
    val ruleLogAlerts = logRawDF
      .filter(col("error_code") === "999")
      .select(
        col("timestamp").alias("alert_time"),
        col("machine_id"),
        lit("Critical Log Error 999").alias("alert_type"),
        lit(999.0).alias("trigger_value"),
        lit(0.0).alias("threshold_value"),
        lit("Check hardware logs").alias("suggested_action")
      )

    // 规则 2：使用 KMeans 聚类发现未知错误模式 (ERR_UNKNOWN)
    // 为了防止流处理中的 ML 模型训练开销过大，我们在 foreachBatch 中对未知错误进行微批次聚类分析
    val unknownLogDF = logRawDF.filter(col("error_code") === "ERR_UNKNOWN")

    unknownLogDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          if (!batchDF.isEmpty) {
            try {
              // 提取错误信息和堆栈用于文本聚类
              val textDF = batchDF.withColumn(
                "full_text",
                concat_ws(" ", col("error_msg"), col("stack_trace"))
              )

              // 文本预处理 (分词)
              val tokenizer = new RegexTokenizer()
                .setInputCol("full_text")
                .setOutputCol("words")
                .setPattern("\\W")
              val wordsDF = tokenizer.transform(textDF)

              // 文本特征化 (TF-IDF)
              val hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures")
                .setNumFeatures(100)
              val featurizedDF = hashingTF.transform(wordsDF)
              val idf =
                new IDF().setInputCol("rawFeatures").setOutputCol("features")
              val idfModel = idf.fit(featurizedDF)
              val rescaledDF = idfModel.transform(featurizedDF)

              // KMeans 聚类 (将未知的错误模式自动划分为 3 类)
              val kmeans = new KMeans()
                .setK(3)
                .setSeed(1L)
                .setFeaturesCol("features")
                .setPredictionCol("cluster_id")
              val model = kmeans.fit(rescaledDF)
              val clusteredDF = model.transform(rescaledDF)

              // 将聚类出来的未知错误模式打上标签并写入 ClickHouse 预警表
              val clusterAlerts = clusteredDF.select(
                col("timestamp").alias("alert_time"),
                col("machine_id"),
                concat(lit("Unknown Pattern Cluster "), col("cluster_id"))
                  .alias("alert_type"),
                col("cluster_id").cast("double").alias("trigger_value"),
                lit(-1.0).alias("threshold_value"), // -1 表示未知阈值
                lit("Investigate unknown log pattern").alias("suggested_action")
              )

              clusterAlerts.write
                .mode("append")
                .jdbc(
                  SparkUtils.ckUrl,
                  "realtime_alerts",
                  SparkUtils.getCkProperties()
                )

              println(
                s"=== 成功将 ${clusterAlerts.count()} 条未知错误模式通过 KMeans 聚类并写入报警表 ==="
              )
            } catch {
              case e: Exception => println(s"KMeans 聚类过程发生异常: ${e.getMessage}")
            }
          }
      }
      .option(
        "checkpointLocation",
        "checkpoints/realtime_alerts_kmeans_cluster"
      )
      .start()

    val combinedAlerts = sensorAlerts.unionByName(ruleLogAlerts)

    // 5. 写入 ClickHouse (任务 16)
    combinedAlerts.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          try {
            batchDF.write
              .mode("append")
              .jdbc(
                SparkUtils.ckUrl,
                "realtime_alerts",
                SparkUtils.getCkProperties()
              )
          } catch { case _: Exception => }
      }
      .option(
        "checkpointLocation",
        "/tmp/checkpoints/realtime_alerts_ck_standalone"
      )
      .start()

    // 6. 推送至 Kafka alert_topic (模块 2.4)
    combinedAlerts
      .selectExpr(
        "CAST(machine_id AS STRING) AS key",
        "to_json(struct(*)) AS value"
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", SparkUtils.kafkaBrokers)
      .option("topic", "alert_topic")
      .option(
        "checkpointLocation",
        "/tmp/checkpoints/realtime_alerts_kafka_standalone"
      )
      .start()

    spark.streams.awaitAnyTermination()
  }
}
