package org.example.tasks

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import redis.clients.jedis.Jedis
import scala.collection.JavaConverters._
import org.example.tasks.AppConfig
import java.sql.Timestamp
import ml.dmlc.xgboost4j.scala.spark.{
  XGBoostClassificationModel,
  XGBoostRegressionModel
}
import org.apache.spark.ml.linalg.Vector

/** 模块二 & 三综合实时引擎 */
object RealtimeEngine {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark = SparkUtils.getSparkSession("RealtimeEngine")
    val hdfsUrl = "hdfs://master:8020"

    // ==========================================
    // 模块 2.1 实时数据接入
    // ==========================================
    val sensorRawDF = SparkUtils
      .getSensorRawStream(spark)
      .withWatermark("timestamp", "1 seconds")

    // 对短时缺失值使用简单插补策略
    // 策略：在 foreachBatch 阶段检测时间间隔，对间隔超过 1 秒但未超过阈值的数据点进行前向填充 + 线性插值近似
    val interpolationMaxGap = AppConfig.getInt("interpolation.max_gap_seconds", 5)
    val interpolationStep = AppConfig.getInt("interpolation.step_seconds", 1)

    // 读 Log 流
    val logRawDF = SparkUtils.getLogRawStream(spark)

    val changeRecordDF = SparkUtils
      .getChangeRecordStream(spark)
      .withWatermark("start_time", "1 seconds")

    // ==========================================
    // 模块 2.2 & 任务 15 & 任务 17 (滑动窗口 & 动态预测)
    // 注意: 任务 17 (实时 RUL 动态预测) 已移至 FlinkRulInference 单独处理
    // 以下保留原始计算逻辑的注释，供参考和后续恢复使用
    // ==========================================
    /*
    // ========== 任务 17 原始代码 (已注释) ==========
    // 实时 RUL 动态预测 (经 Window 计算特征)
    val dfWithFaultFeat = batchDF
      .withColumn("avg_temp", col("max_temperature"))
      .withColumn(
        "var_temp",
        when(col("current_fluctuation").isNull, lit(0.0))
          .otherwise(col("current_fluctuation"))
      )
      .withColumn("kurtosis_temp", lit(0.5))
      .withColumn("fft_peak", col("avg_vib_rms") * 100)

    val enrichedWindowDF = dfWithFaultFeat
      .withColumn("hash_val", abs(hash(col("machine_id")) % 50))
      .withColumn(
        "fault_probability",
        when(
          col("avg_temp") > 80.0,
          lit(0.7) + (col("hash_val") / 500.0)
        )
          .otherwise(lit(0.1) + (col("hash_val") / 100.0))
      )
      .withColumn(
        "rul_hours",
        lit(250.0) - (col("avg_temp") * 1.5) - (col("hash_val") * 2.0)
      )
      .withColumn(
        "risk_level",
        when(col("rul_hours") < 48.0, "High")
          .when(col("rul_hours") < 168.0, "Medium")
          .otherwise("Low")
      )

    val ckRulDF = enrichedWindowDF.select(
      col("window_end").alias("predict_time"),
      col("machine_id"),
      col("rul_hours").alias("predicted_rul"),
      col("risk_level"),
      col("fault_probability").alias("failure_probability"),
      current_timestamp().alias("insert_time")
    )
    ckRulDF.write
      .mode("append")
      .jdbc(
        SparkUtils.ckUrl,
        "realtime_rul_monitor",
        SparkUtils.getCkProperties()
      )
    // ========== 任务 17 原始代码结束 ==========
    */
    val aggCols = Seq(
      max("temperature").alias("max_temperature"),
      sqrt(
        mean(
          col("vibration_x") * col("vibration_x") + col("vibration_y") * col(
            "vibration_y"
          ) + col("vibration_z") * col("vibration_z")
        )
      ).alias("avg_vib_rms"),
      stddev("current").alias("current_fluctuation"),
      (max("temperature") - min("temperature")).alias("temp_slope")
    )

    val window1mDF = sensorRawDF
      .groupBy(
        window(col("timestamp"), "1 minute", "1 minute"),
        col("machine_id")
      )
      .agg(aggCols.head, aggCols.tail: _*)
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("machine_id"),
        col("max_temperature"),
        col("avg_vib_rms"),
        col("current_fluctuation"),
        col("temp_slope"),
        lit("1m").alias("window_type")
      )

    window1mDF.writeStream
      .outputMode("update")
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          if (!batchDF.isEmpty) {
            try {
              // ========== 线性插值补缺失值 ==========
              // 检测同一设备相邻数据点的时间间隔，对间隔 > interpolationStep 秒但 < interpolationMaxGap 秒的情况
              // 进行前向填充插值，避免短时空洞导致窗口聚合不准
              val sparkSession = batchDF.sparkSession
              // 注意：不在此处重新 import sparkSession.implicits._，避免与外层冲突
              // 使用全限定名引用 Window

              val interpolatedDF = if (!batchDF.isEmpty && interpolationMaxGap > 0) {
                try {
                  // 窗口内按设备+时间排序，计算与前一条记录的时间差（秒）
                  val withLag = batchDF
                    .withColumn("prev_timestamp", lag("window_end", 1).over(
                      Window.partitionBy("machine_id").orderBy("window_end")
                    ))
                    .withColumn("time_gap_sec",
                      unix_timestamp(col("window_end")) - unix_timestamp(col("prev_timestamp"))
                    )

                  // 对时间间隔在 (interpolationStep, interpolationMaxGap] 范围内的空隙进行标记
                  // 这里采用策略：将 gap 较大的窗口的指标用前值填充（forward fill 近似）
                  val filled = withLag
                    .withColumn("filled_max_temperature",
                      when(col("time_gap_sec") > interpolationStep && col("time_gap_sec") <= interpolationMaxGap,
                        lag("max_temperature", 1).over(Window.partitionBy("machine_id").orderBy("window_end")))
                        .otherwise(col("max_temperature"))
                    )
                    .withColumn("filled_avg_vib_rms",
                      when(col("time_gap_sec") > interpolationStep && col("time_gap_sec") <= interpolationMaxGap,
                        lag("avg_vib_rms", 1).over(Window.partitionBy("machine_id").orderBy("window_end")))
                        .otherwise(col("avg_vib_rms"))
                    )
                    .withColumn("filled_current_fluctuation",
                      when(col("time_gap_sec") > interpolationStep && col("time_gap_sec") <= interpolationMaxGap,
                        lag("current_fluctuation", 1).over(Window.partitionBy("machine_id").orderBy("window_end")))
                        .otherwise(col("current_fluctuation"))
                    )

                  filled.select(
                    col("window_start"), col("window_end"), col("machine_id"),
                    coalesce(col("filled_max_temperature"), col("max_temperature")).alias("max_temperature"),
                    coalesce(col("filled_avg_vib_rms"), col("avg_vib_rms")).alias("avg_vib_rms"),
                    coalesce(col("filled_current_fluctuation"), col("current_fluctuation")).alias("current_fluctuation"),
                    col("temp_slope"), col("window_type")
                  )
                } catch { case _: Exception => batchDF }
              } else { batchDF }

              // 任务 15: 写入 ClickHouse realtime_status_window（使用插值后的数据）
              val outputDF = interpolatedDF.select(
                col("window_end"),
                col("machine_id"),
                col("max_temperature"),
                col("avg_vib_rms")
              )
              // 在写入 ClickHouse 前检查振动突增（与上一个窗口比较），并发送报警到 ClickHouse/Kafka
              outputDF.foreachPartition { (iter: Iterator[org.apache.spark.sql.Row]) =>
                val jedis = new Jedis(AppConfig.getString("redis.host", "master"), AppConfig.getInt("redis.port", 6379))
                val alerts = scala.collection.mutable.ArrayBuffer.empty[org.apache.spark.sql.Row]
                iter.foreach { row =>
                  try {
                    val machineId = row.getAs[String]("machine_id")
                    val curVib = row.getAs[Double]("avg_vib_rms")
                    val windowEnd = row.getAs[java.sql.Timestamp]("window_end")
                    val prevKey = s"prev_vib_rms:$machineId"
                    val prevStr = jedis.get(prevKey)
                    if (prevStr != null) {
                      val prev = prevStr.toDouble
                      if (prev > 0.0) {
                        val pct = math.abs(curVib - prev) / prev
                        if (pct >= AppConfig.getDouble("vibration.spike.percent", 0.2)) {
                          // 触发报警记录，构造一个 Row 等待后续统一写入
                          val alertRow = org.apache.spark.sql.RowFactory.create(
                            windowEnd,
                            machineId,
                            s"振动突增:${(pct * 100).formatted("%.2f")}%%",
                            curVib.asInstanceOf[AnyRef]
                          )
                          alerts += alertRow
                        }
                      }
                    }
                    // 更新 prev value
                    jedis.set(prevKey, curVib.toString)
                  } catch {
                    case _: Exception => // ignore per-row
                  }
                }
                // 写 ClickHouse：将收集的 alerts 批量写入
                if (alerts.nonEmpty) {
                  try {
                    val sparkSession = org.apache.spark.sql.SparkSession.builder.getOrCreate()
                    import sparkSession.implicits._
                    val alertRows = alerts.toSeq
                    val schema = org.apache.spark.sql.types.StructType(Seq(
                      org.apache.spark.sql.types.StructField("alert_time", org.apache.spark.sql.types.TimestampType, true),
                      org.apache.spark.sql.types.StructField("machine_id", org.apache.spark.sql.types.StringType, true),
                      org.apache.spark.sql.types.StructField("alert_type", org.apache.spark.sql.types.StringType, true),
                      org.apache.spark.sql.types.StructField("trigger_value", org.apache.spark.sql.types.DoubleType, true)
                    ))
                    val rdd = sparkSession.sparkContext.parallelize(alertRows)
                    val alertDF = sparkSession.createDataFrame(rdd, schema)
                    // 写入 ClickHouse realtime_alerts
                    alertDF.write
                      .mode("append")
                      .jdbc(
                        SparkUtils.ckUrl,
                        "realtime_alerts",
                        SparkUtils.getCkProperties()
                      )
                    // 也发到 Kafka alert_topic
                    try {
                      alertDF.selectExpr("CAST(machine_id AS STRING) AS key", "to_json(struct(*)) AS value")
                        .write
                        .format("kafka")
                        .option("kafka.bootstrap.servers", SparkUtils.kafkaBrokers)
                        .option("topic", AppConfig.getString("kafka.alert_topic", "alert_topic"))
                        .save()
                    } catch { case _: Exception => }
                  } catch { case _: Exception => }
                }
                jedis.close()
              }

              // 最终写入 ClickHouse
              outputDF.write
                .mode("append")
                .jdbc(
                  SparkUtils.ckUrl,
                  "realtime_status_window",
                  SparkUtils.getCkProperties()
                )
            } catch { case e: Exception => e.printStackTrace() }
          }
      }
      .option("checkpointLocation", "checkpoints/engine_status_window")
      .start()

    val window5mDF = sensorRawDF
      .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("machine_id")
      )
      .agg(aggCols.head, aggCols.tail: _*)
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("machine_id"),
        col("max_temperature"),
        col("avg_vib_rms"),
        col("current_fluctuation"),
        col("temp_slope"),
        lit("5m").alias("window_type")
      )
    val window1hDF = sensorRawDF
      .groupBy(
        window(col("timestamp"), "1 hour", "5 minutes"),
        col("machine_id")
      )
      .agg(aggCols.head, aggCols.tail: _*)
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("machine_id"),
        col("max_temperature"),
        col("avg_vib_rms"),
        col("current_fluctuation"),
        col("temp_slope"),
        lit("1h").alias("window_type")
      )
    // 5分钟窗口：写入 ClickHouse realtime_status_window
    window5mDF.writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          try {
            batchDF.select(
              col("window_end"),
              col("machine_id"),
              col("max_temperature"),
              col("avg_vib_rms")
            ).write
              .mode("append")
              .jdbc(SparkUtils.ckUrl, "realtime_status_window", SparkUtils.getCkProperties())
          } catch { case e: Exception => e.printStackTrace() }
        }
      }
      .option("checkpointLocation", "checkpoints/engine_status_window_5m")
      .start()

    // 1小时窗口：写入 ClickHouse realtime_status_window
    window1hDF.writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          try {
            batchDF.select(
              col("window_end"),
              col("machine_id"),
              col("max_temperature"),
              col("avg_vib_rms")
            ).write
              .mode("append")
              .jdbc(SparkUtils.ckUrl, "realtime_status_window", SparkUtils.getCkProperties())
          } catch { case e: Exception => e.printStackTrace() }
        }
      }
      .option("checkpointLocation", "checkpoints/engine_status_window_1h")
      .start()

    // ==========================================
    // 模块 2.2 状态机识别与流计算
    // ==========================================
    println(">>> [1/6] 启动 ChangeRecord 流 (状态机识别)")
    changeRecordDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          // 收集到 Driver 端处理，避免在 Executor 中创建不可序列化的 Jedis
          println(s"[批次 $batchId] ChangeRecord 触发")
          val rows = batchDF.collect()
          println(s"[批次 $batchId] ChangeRecord 数据条数: ${rows.length}")
          if (rows.nonEmpty) {
            println(s"[批次 $batchId] 开始写入 Redis")
            val jedis = new Jedis("master", 6379)
            try {
              rows.foreach { row =>
                try {
                  val mid = row.getAs[String]("machine_id")
                  val status = row.getAs[String]("status")
                  if (mid != null && status != null && !mid.isEmpty && !status.isEmpty) {
                    // 将设备状态写入 Redis，供 sensorRaw 流查询设备运行状态
                    jedis.hset("change_record_status", mid, status)
                  }
                } catch { case e: Exception => println(s"[警告] Redis写入失败: ${e.getMessage}") }
              }
            } finally {
              jedis.close()
            }
          }
      }
      .outputMode("update")
      .option("checkpointLocation", "checkpoints/engine_change_record_writer")
      .start()

    println(">>> [2/6] 启动 SensorRaw 流 (实时指标计算)")
    sensorRawDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          println(s"[批次 $batchId] SensorRaw 触发")
          val count = batchDF.count()
          println(s"[批次 $batchId] 数据条数: $count")
          if (count > 0) {
            println(s"[批次 $batchId] 开始处理 $count 条 sensor_raw 数据")
            val sparkSession = batchDF.sparkSession
            import sparkSession.implicits._

            // 收集到 Driver 端处理，避免 Jedis 序列化问题
            val sensorRows = batchDF.collect()
            val jedis = new Jedis("master", 6379)
            val statusMap = scala.collection.mutable.Map[String, String]()
            try {
              sensorRows.foreach { row =>
                val mid =
                  if (row.isNullAt(row.fieldIndex("machine_id"))) null
                  else row.getAs[String]("machine_id")
                if (mid != null) {
                  val status = jedis.hget("change_record_status", mid)
                  statusMap(mid) = if (status == null) "" else status
                }
              }
            } finally {
              jedis.close()
            }
            
            import sparkSession.implicits._
            val withStatusDF = sensorRows.toSeq.map { row =>
              val mid =
                if (row.isNullAt(row.fieldIndex("machine_id"))) null
                else row.getAs[String]("machine_id")
              (
                mid,
                row.getAs[Timestamp]("timestamp"),
                row.getAs[Double]("temperature"),
                row.getAs[Double]("vibration_x"),
                row.getAs[Double]("speed"),
                statusMap.getOrElse(mid, "")
              )
            }.toDF(
              "machine_id",
              "timestamp",
              "temperature",
              "vibration_x",
              "speed",
              "record_status"
            )

            // =========================================================================
            // 由于 Flink 专门负责模型预测，这里只需要聚合实时指标存入 ClickHouse
            // 注意: 以下保留 fault_probability 和 rul_hours 计算逻辑的注释
            // =========================================================================
            /*
            // ========== fault_probability 和 rul_hours 原始代码 (已注释) ==========
            val enrichedDF = withStatusDF
              .withColumn("hash_val", abs(hash(col("machine_id")) % 50))
              .withColumn(
                "fault_probability",
                // 基于温度动态计算概率，最高不超过 0.82 以防止疯狂报警，只有极少数情况突破 0.85
                when(
                  col("temperature") > 80.0,
                  lit(0.7) + (col("hash_val") / 500.0)
                )
                  .otherwise(lit(0.1) + (col("hash_val") / 100.0))
              )
              .withColumn(
                "rul_hours",
                // 剩余寿命也用公式计算
                lit(250.0) - (col("temperature") * 1.5) - (col(
                  "hash_val"
                ) * 2.0)
              )
              .withColumn(
                "machine_status",
                when(col("record_status") === "预警", lit("异常停机"))
                  .when(col("speed") < 1000, lit("启动中"))
                  .otherwise(lit("稳定运行"))
              )
              .withColumn(
                "rul_prediction",
                when(col("machine_status") === "稳定运行", col("rul_hours"))
                  .otherwise(lit(null))
              )
              .withColumn(
                "health_score",
                when(
                  col("machine_status") === "稳定运行",
                  lit(100.0) - col("fault_probability") * 100
                )
                  .when(col("machine_status") === "启动中", lit(80.0))
                  .otherwise(lit(60.0))
              )
            // ========== fault_probability 和 rul_hours 原始代码结束 ==========
            */
            val enrichedDF = withStatusDF
              .withColumn(
                "machine_status",
                when(col("record_status") === "预警", lit("异常停机"))
                  .when(col("speed") < 1000, lit("启动中"))
                  .otherwise(lit("稳定运行"))
              )
              .withColumn(
                "health_score",
                when(col("machine_status") === "稳定运行", lit(100.0))
                  .when(col("machine_status") === "启动中", lit(80.0))
                  .otherwise(lit(60.0))
              )

            // 写入 Redis，但限制每设备更新频率为每分钟一次，以减少 Redis 压力并保证"每分钟快照"语义
            // 在 Driver 端处理，避免 Jedis 序列化问题
            val healthRows = enrichedDF.collect()
          println(s"[批次 $batchId] 健康数据条数: ${healthRows.length}")
          if (healthRows.nonEmpty) {
            println(s"[批次 $batchId] 开始写入 Redis/ClickHouse")
            val jedis = new Jedis(AppConfig.getString("redis.host", "master"), AppConfig.getInt("redis.port", 6379))
              try {
                val nowSec = System.currentTimeMillis() / 1000
                val minInterval = AppConfig.getInt("device_health.min_update_seconds", 60)
                healthRows.foreach { row =>
                  try {
                    if (!row.isNullAt(row.fieldIndex("machine_id"))) {
                      val mid = row.getAs[String]("machine_id")
                      val score = row.getAs[Double]("health_score")
                      val lastKey = s"device_health_update_time:$mid"
                      val lastStr = jedis.get(lastKey)
                      val doUpdate = if (lastStr == null) true
                      else {
                        try {
                          val last = lastStr.toLong
                          (nowSec - last) >= minInterval
                        } catch { case _: Exception => true }
                      }
                      if (doUpdate) {
                        jedis.hset("device_health", mid, score.toString)
                        jedis.set(lastKey, nowSec.toString)
                      }
                    }
                  } catch { case e: Exception => println(s"[警告] Redis健康分写入失败: ${e.getMessage}") }
                }
              } finally {
                jedis.close()
              }
            }

            // 写入 ClickHouse device_realtime_status
            val ckRealtimeStatusDF = enrichedDF.select(
              col("machine_id"),
              col("timestamp").alias("update_time"),
              col("machine_status"),
              col("temperature").alias("current_temperature"),
              col("vibration_x").alias("current_vibration_rms"),
              col("health_score")
            )
            try {
              ckRealtimeStatusDF.write
                .mode("append")
                .jdbc(
                  SparkUtils.ckUrl,
                  "device_realtime_status",
                  SparkUtils.getCkProperties()
                )
              println(s"[CK写入] device_realtime_status: ${ckRealtimeStatusDF.count()} 条")
            } catch { case e: Exception => println(s"[错误] ClickHouse写入失败: ${e.getMessage}") }

            // 动态阈值报警
            // 注意: 以下保留 RUL < 48 和 fault_probability > 0.85 条件的注释
            /*
            // ========== 动态阈值报警原始代码 (已注释) ==========
            val mlAlerts = enrichedDF
              .filter(
                col("temperature") > 80.0 || col("vibration_x") > 2.0 || col(
                  "rul_hours"
                ) < 48.0 || col("fault_probability") > 0.85
              )
              .select(
                col("timestamp").alias("alert_time"),
                col("machine_id"),
                when(col("temperature") > 80.0, "高温预警")
                  .when(col("fault_probability") > 0.85, "故障概率超标(>85%)")
                  .when(col("rul_hours") < 48.0, "寿命不足预警(RUL<48h)")
                  .otherwise("振动异常")
                  .alias("alert_type"),
                when(col("temperature") > 80.0, col("temperature"))
                  .when(
                    col("fault_probability") > 0.85,
                    col("fault_probability")
                  )
                  .when(col("rul_hours") < 48.0, col("rul_hours"))
                  .otherwise(col("vibration_x"))
                  .alias("trigger_value"),
                when(col("temperature") > 80.0, lit(80.0))
                  .when(col("fault_probability") > 0.85, lit(0.85))
                  .when(col("rul_hours") < 48.0, lit(48.0))
                  .otherwise(lit(2.0))
                  .alias("threshold_value"),
                lit("请立即检查设备及模型预测结果").alias("suggested_action"),
                current_timestamp().alias("insert_time")
              )
            // ========== 动态阈值报警原始代码结束 ==========
            */
            val tempThreshold = AppConfig.getDouble("threshold.temperature", 80.0)
            val vibThreshold = AppConfig.getDouble("threshold.vibration", 2.0)
            val mlAlerts = enrichedDF
              .filter(
                col("temperature") > tempThreshold || col("vibration_x") > vibThreshold
              )
              .select(
                col("timestamp").alias("alert_time"),
                col("machine_id"),
                when(col("temperature") > tempThreshold, "高温预警")
                  .otherwise("振动异常")
                  .alias("alert_type"),
                when(col("temperature") > tempThreshold, col("temperature"))
                  .otherwise(col("vibration_x"))
                  .alias("trigger_value"),
                when(col("temperature") > tempThreshold, lit(tempThreshold))
                  .otherwise(lit(vibThreshold))
                  .alias("threshold_value"),
                lit("请立即检查设备").alias("suggested_action"),
                current_timestamp().alias("insert_time")
              )

            try {
              mlAlerts.write
                .mode("append")
                .jdbc(
                  SparkUtils.ckUrl,
                  "realtime_alerts",
                  SparkUtils.getCkProperties()
                )
            } catch { case _: Exception => }

            try {
              mlAlerts
                .selectExpr(
                  "CAST(machine_id AS STRING) AS key",
                  "to_json(struct(*)) AS value"
                )
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", SparkUtils.kafkaBrokers)
                .option("topic", "alert_topic")
                .save()
            } catch { case _: Exception => }
          }
      }
      .outputMode("append")
      .option("checkpointLocation", "checkpoints/engine_enriched")
      .start()

    // ==========================================
    // 日志异常检测 (独立流)
    // ==========================================
    println(">>> [3/6] 启动 LogRaw 流 (异常检测)")
    logRawDF
      .filter(col("error_code") === "999")
      .select(
        col("timestamp").alias("alert_time"),
        col("machine_id"),
        lit("严重错误: 999").alias("alert_type"),
        lit(999.0).alias("trigger_value"),
        lit(0.0).alias("threshold_value"),
        lit("请立即检查硬件日志").alias("suggested_action"),
        current_timestamp().alias("insert_time")
      )
      .writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          if (!batchDF.isEmpty) {
            try {
              batchDF.write
                .mode("append")
                .jdbc(
                  SparkUtils.ckUrl,
                  "realtime_alerts",
                  SparkUtils.getCkProperties()
                )
            } catch { case _: Exception => }
            try {
              batchDF
                .selectExpr(
                  "CAST(machine_id AS STRING) AS key",
                  "to_json(struct(*)) AS value"
                )
                .write
                .format("kafka")
                .option("kafka.bootstrap.servers", SparkUtils.kafkaBrokers)
                .option("topic", "alert_topic")
                .save()
            } catch { case _: Exception => }
          }
      }
      .option("checkpointLocation", "checkpoints/engine_log_alerts")
      .start()

    // ==========================================
    // 任务 18 实时工艺参数偏离监控
    // ==========================================
    println(">>> [4/6] 启动工艺参数偏离监控流")
    sensorRawDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          val sparkSession = batchDF.sparkSession
          import sparkSession.implicits._
          val devDF = batchDF
            .mapPartitions { iter =>
              val jedis = new Jedis("master", 6379)
              val res = iter
                .map { row =>
                  val mid = row.getAs[String]("machine_id")
                  val current = row.getAs[Double]("current")
                  val speed = row.getAs[Double]("speed")
                  val stdCurrentStr = jedis.hget(s"std_params:$mid", "current")
                  val stdSpeedStr = jedis.hget(s"std_params:$mid", "speed")
                  val stdCurrent =
                    if (stdCurrentStr != null) stdCurrentStr.toDouble else 30.0
                  val stdSpeed =
                    if (stdSpeedStr != null) stdSpeedStr.toDouble else 3000.0
                  val currentDev = Math.abs(current - stdCurrent) / stdCurrent
                  val speedDev = Math.abs(speed - stdSpeed) / stdSpeed
                  (
                    mid,
                    currentDev,
                    speedDev,
                    row.getAs[Timestamp]("timestamp"),
                    current,
                    stdCurrent
                  )
                }
                  .filter(x => x._2 > AppConfig.getDouble("deviation.threshold", 0.028) || x._3 > AppConfig.getDouble("deviation.threshold", 0.028))
              jedis.close()
              res
            }
            .toDF(
              "machine_id",
              "current_dev",
              "speed_dev",
              "timestamp",
              "actual_value",
              "standard_value"
            )

          devDF.createOrReplaceTempView("temp_dev")
          val aggDevDF = sparkSession.sql("""
      SELECT window.start as record_time, machine_id, 'current' as param_name,
             ROUND(MAX(actual_value), 2) as actual_value, 
             ROUND(MAX(standard_value), 2) as standard_value,
             ROUND(MAX(current_dev)*100, 2) as deviation_ratio, 
             30 as duration_seconds
      FROM (SELECT *, window(timestamp, '30 seconds', '10 seconds') as window FROM temp_dev)
      GROUP BY machine_id, window
      """)

          try {
            aggDevDF.write
              .mode("append")
              .jdbc(
                SparkUtils.ckUrl,
                "realtime_process_deviation",
                SparkUtils.getCkProperties()
              )
          } catch { case e: Exception => e.printStackTrace() }
      }
      .option("checkpointLocation", "checkpoints/engine_process_dev")
      .start()

    // ==========================================
    // 模块 4.1 实时传感器数据入湖 (Hudi)
    // ==========================================
    println(">>> [5/6] 启动 Sensor Hudi 入湖流")
    sensorRawDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          if (!batchDF.isEmpty) {
            try {
              batchDF.write
                .format("hudi")
                .option(
                  "hoodie.table.name",
                  "sensor_detail_realtime"
                ) // 这里的表名对应 hudi 库表名
                .option(
                  "hoodie.datasource.write.recordkey.field",
                  "machine_id,ts"
                )
                .option(
                  "hoodie.datasource.write.precombine.field",
                  "ts"
                )
                .option("hoodie.datasource.write.operation", "upsert")
                .option(
                  "hoodie.datasource.write.table.type",
                  "MERGE_ON_READ"
                ) // MOR 表
                .option("hoodie.compact.inline", "false") // 异步压缩
                .option(
                  "hoodie.compact.inline.max.delta.commits",
                  "5"
                ) // 每 5 个 commits 压缩一次
                .mode("append")
                .save(s"$hdfsUrl/hudi/dwd/sensor_detail_realtime")
            } catch { case e: Exception => e.printStackTrace() }
          }
      }
      .option("checkpointLocation", "checkpoints/hudi_sensor_sink")
      .start()

    // ==========================================
    // 模块 4.2 实时日志入湖 (Hudi)
    // ==========================================
    println(">>> [6/6] 启动 Log Hudi 入湖流")
    logRawDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          if (!batchDF.isEmpty) {
            try {
              val enrichedLogDF = batchDF
                .withColumn(
                  "severity",
                  when(col("error_code") === "999", "Critical")
                    .when(col("error_code") === "500", "High")
                    .otherwise("Normal")
                )

              enrichedLogDF.write
                .format("hudi")
                .option("hoodie.table.name", "device_log_realtime")
                .option(
                  "hoodie.datasource.write.recordkey.field",
                  "machine_id,ts"
                )
                .option("hoodie.datasource.write.precombine.field", "ts")
                .option("hoodie.datasource.write.operation", "upsert")
                .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
                .option("hoodie.compact.inline", "false")
                .option("hoodie.compact.inline.max.delta.commits", "5")
                .mode("append")
                .save(s"$hdfsUrl/hudi/dwd/device_log_realtime")
            } catch { case e: Exception => e.printStackTrace() }
          }
      }
      .option("checkpointLocation", "checkpoints/hudi_log_sink")
      .start()

    println(">>> 所有流已启动，等待数据...")
    println("=" * 70)
    spark.streams.awaitAnyTermination()
  }
}
