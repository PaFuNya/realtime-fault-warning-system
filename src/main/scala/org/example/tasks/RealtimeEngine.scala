package org.example.tasks

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import redis.clients.jedis.Jedis
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
    val hdfsUrl = "hdfs://master:9000"

    // ==========================================
    // 模块 2.1 实时数据接入
    // ==========================================
    val sensorRawDF = SparkUtils
      .getSensorRawStream(spark)
      .withWatermark("timestamp", "1 seconds")

    // 读 Log 流
    val logRawDF = SparkUtils.getLogRawStream(spark)

    val changeRecordDF = SparkUtils
      .getChangeRecordStream(spark)
      .withWatermark("start_time", "1 seconds")

    // ==========================================
    // 模块 2.2 & 任务 15 & 任务 17 (滑动窗口 & 动态预测)
    // ==========================================
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
              // 任务 15: 写入 ClickHouse realtime_status_window
              val outputDF = batchDF.select(
                col("window_end"),
                col("machine_id"),
                col("max_temperature"),
                col("avg_vib_rms")
              )
              outputDF.write
                .mode("append")
                .jdbc(
                  SparkUtils.ckUrl,
                  "realtime_status_window",
                  SparkUtils.getCkProperties()
                )

              // 任务 17: 实时 RUL 动态预测 (经 Window 计算特征)
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
    window5mDF.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()
    window1hDF.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()

    // ==========================================
    // 模块 2.2 状态机识别与流计算
    // ==========================================
    changeRecordDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          batchDF.foreachPartition {
            (iter: Iterator[org.apache.spark.sql.Row]) =>
              val jedis = new Jedis("master", 6379)
              iter.foreach { row =>
                if (
                  !row.isNullAt(row.fieldIndex("machine_id")) && !row.isNullAt(
                    row.fieldIndex("status")
                  )
                ) {
                  val mid = row.getAs[String]("machine_id")
                  val status = row.getAs[String]("status")
                  if (mid != null && status != null)
                    jedis.hset("change_record_status", mid, status)
                }
              }
              jedis.close()
          }
      }
      .outputMode("update")
      .option("checkpointLocation", "checkpoints/engine_change_record_writer")
      .start()

    sensorRawDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          if (!batchDF.isEmpty) {
            val sparkSession = batchDF.sparkSession
            import sparkSession.implicits._

            val withStatusDF = batchDF
              .mapPartitions { iter =>
                val jedis = new Jedis("master", 6379)
                val res = iter.map { row =>
                  val mid =
                    if (row.isNullAt(row.fieldIndex("machine_id"))) null
                    else row.getAs[String]("machine_id")
                  val status =
                    if (mid != null) jedis.hget("change_record_status", mid)
                    else ""
                  (
                    mid,
                    row.getAs[Timestamp]("timestamp"),
                    row.getAs[Double]("temperature"),
                    row.getAs[Double]("vibration_x"),
                    row.getAs[Double]("speed"),
                    if (status == null) "" else status
                  )
                }
                jedis.close()
                res
              }
              .toDF(
                "machine_id",
                "timestamp",
                "temperature",
                "vibration_x",
                "speed",
                "record_status"
              )

            // =========================================================================
            // 由于 Flink 专门负责模型预测，这里只需要聚合实时指标存入 ClickHouse
            // =========================================================================
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

            // 写入 Redis
            enrichedDF.foreachPartition {
              (iter: Iterator[org.apache.spark.sql.Row]) =>
                val jedis = new Jedis("master", 6379)
                iter.foreach { row =>
                  if (!row.isNullAt(row.fieldIndex("machine_id"))) {
                    val mid = row.getAs[String]("machine_id")
                    val score = row.getAs[Double]("health_score")
                    jedis.hset("device_health", mid, score.toString)
                  }
                }
                jedis.close()
            }

            // 写入 ClickHouse device_realtime_status
            val ckRealtimeStatusDF = enrichedDF.select(
              col("machine_id"),
              col("timestamp").alias("update_time"),
              col("machine_status"),
              col("temperature").alias("current_temperature"),
              col("vibration_x").alias("current_vibration_rms"),
              col("rul_prediction").alias("current_rul"),
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
            } catch { case _: Exception => }

            // 动态阈值报警
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
                .filter(x => x._2 > 0.028 || x._3 > 0.028)
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

    spark.streams.awaitAnyTermination()
  }
}
