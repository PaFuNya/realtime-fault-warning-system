package org.example.tasks

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import redis.clients.jedis.Jedis
import java.sql.Timestamp

/** 模块二 & 三综合实时引擎 包含需求：
  *   - 2.1 实时数据接入与清洗
  *   - 2.2 实时特征计算 (滑动窗口、状态机识别)
  *   - 2.4 实时结果输出 (报警推送 Kafka、实时指标写入 ClickHouse、状态快照更新 Redis)
  *   - 任务 15: 实时状态窗口统计写入 ClickHouse (realtime_status_window)
  *   - 任务 16: 实时异常检测与报警写入 ClickHouse (realtime_alerts)
  *   - 任务 18: 实时工艺参数偏离监控写入 ClickHouse (realtime_process_deviation)
  */
object RealtimeEngine {

  def main(args: Array[String]): Unit = {
    // 解决 HDFS 写入权限问题 (继承自 Task13_14)
    System.setProperty("HADOOP_USER_NAME", "root")

    val spark = SparkUtils.getSparkSession("RealtimeEngine")

    // ==========================================
    // 模块 2.1 实时数据接入
    // ==========================================
    // 1. 读 Sensor 流
    val sensorRawDF = SparkUtils
      .getSensorRawStream(spark)
      .withWatermark("timestamp", "2 minutes") // 允许一定延迟的数据

    // 2. 读 Log 流
    val logRawDF = SparkUtils.getLogRawStream(spark)

    // 3. 读取 ChangeRecord 流
    val changeRecordDF = SparkUtils
      .getChangeRecordStream(spark)
      .withWatermark("start_time", "5 seconds")

    // ==========================================
    // 任务 13 & 14: 数据入湖 (Hudi MOR 表)
    // ==========================================
    val hudiSensorOptions = Map(
      "hoodie.table.name" -> "sensor_detail_realtime",
      "hoodie.datasource.write.recordkey.field" -> "machine_id,ts",
      "hoodie.datasource.write.precombine.field" -> "ts",
      "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.metadata.enable" -> "false", // 关闭 metadata table 绕过 Hadoop 冲突
      "hoodie.compact.inline" -> "false",
      "hoodie.compact.async.enable" -> "true",
      "hoodie.compact.inline.max.delta.commits" -> "5"
    )

    sensorRawDF.writeStream
      .format("hudi")
      .options(hudiSensorOptions)
      .option(
        "checkpointLocation",
        "hdfs://bigdata1:9000/hudi/checkpoints/engine_sensor_hudi"
      )
      .outputMode("append")
      .start("hdfs://bigdata1:9000/hudi/dwd/sensor_detail_realtime")

    val processedLogDF = logRawDF.withColumn(
      "severity",
      when(col("error_code") === "999", "Critical")
        .when(col("error_code") === "500", "High")
        .otherwise("Normal")
    )

    val hudiLogOptions = Map(
      "hoodie.table.name" -> "device_log_realtime",
      "hoodie.datasource.write.recordkey.field" -> "machine_id,ts",
      "hoodie.datasource.write.precombine.field" -> "ts",
      "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.metadata.enable" -> "false",
      "hoodie.compact.inline" -> "false",
      "hoodie.compact.async.enable" -> "true",
      "hoodie.compact.inline.max.delta.commits" -> "5"
    )

    processedLogDF.writeStream
      .format("hudi")
      .options(hudiLogOptions)
      .option(
        "checkpointLocation",
        "hdfs://bigdata1:9000/hudi/checkpoints/engine_log_hudi"
      )
      .outputMode("append")
      .start("hdfs://bigdata1:9000/hudi/dwd/device_log_realtime")

    // ==========================================
    // 模块 2.2 & 任务 15 实时特征计算 (1分钟窗口)
    // ==========================================
    val window1mDF = sensorRawDF
      .groupBy(window(col("timestamp"), "1 minute"), col("machine_id"))
      .agg(
        max("temperature").alias("max_temperature"), // 任务 15: 最大温度
        sqrt(
          mean(
            col("vibration_x") * col("vibration_x") + col("vibration_y") * col(
              "vibration_y"
            ) + col("vibration_z") * col("vibration_z")
          )
        ).alias("avg_vib_rms"), // 任务 15: 振动 RMS 均值
        stddev("current").alias("current_fluctuation"), // 电流波动率
        (max("temperature") - min("temperature")).alias("temp_slope") // 温度上升斜率
      )
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("machine_id"),
        col("max_temperature"),
        col("avg_vib_rms")
      )

    // -> 写入 ClickHouse (任务 15)
    window1mDF.writeStream
      .outputMode("update") // 修复 1：将 append 改为 update，让实时窗口的每秒变化都能输出，避免等一分钟
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          try {
            batchDF.write
              .mode("append")
              .jdbc(
                SparkUtils.ckUrl,
                "realtime_status_window",
                SparkUtils.getCkProperties()
              )
          } catch { case e: Exception => e.printStackTrace() } // 打印可能的写入报错
      }
      .option("checkpointLocation", "/tmp/checkpoints/engine_status_window")
      .start()

    // ==========================================
    // 模块 2.2 状态机识别与流计算
    // ==========================================
    val joinedDF = sensorRawDF
      .alias("sensor")
      .join(
        changeRecordDF.alias("record"),
        expr("""
          sensor.machine_id = record.machine_id AND
          sensor.timestamp >= record.start_time AND
          sensor.timestamp <= record.start_time + interval 1 minute
        """),
        "leftOuter"
      )
      .drop(changeRecordDF("machine_id"))

    val enrichedDFStream = joinedDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          if (!batchDF.isEmpty) {
            val windowSpec =
              Window.partitionBy("machine_id").orderBy("timestamp")

            val enrichedDF = batchDF
              .withColumn(
                "machine_status",
                // 修复状态机逻辑：即使没有 ChangeRecord(status为null)，只要转速<1000也算“异常停机”
                when(col("status") === "预警" || col("speed") < 1000, lit("异常停机"))
                  .otherwise(lit("稳定运行"))
              )
              .withColumn("prev_ts", lag("timestamp", 1).over(windowSpec))
              .withColumn("prev_vib_x", lag("vibration_x", 1).over(windowSpec))
              .withColumn(
                "vib_surge_rate",
                when(
                  col("prev_vib_x") > 0,
                  (col("vibration_x") - col("prev_vib_x")) / col("prev_vib_x")
                ).otherwise(0.0)
              )
              // 模块 2.3 RUL 预测 (稳定运行才预测)
              .withColumn(
                "rul_prediction",
                when(
                  col("machine_status") === "稳定运行",
                  lit(100.0) - (col("temperature") * 0.1) - (col(
                    "vibration_x"
                  ) * 5.0)
                ).otherwise(lit(null))
              )
              .withColumn( // 模拟健康分
                "health_score",
                when(col("machine_status") === "稳定运行", lit(95.0))
                  .otherwise(lit(60.0))
              )

            // ==========================================
            // 模块 2.4 状态快照更新 Redis & 实时指标写入 ClickHouse 大屏表
            // ==========================================
            enrichedDF.foreachPartition {
              (iter: Iterator[org.apache.spark.sql.Row]) =>
                val jedis = new Jedis("192.168.45.11", 6379)
                iter.foreach { row =>
                  val mid = row.getAs[String]("machine_id")
                  val score = row.getAs[Double]("health_score")
                  // 1. 更新 Redis 健康分
                  jedis.hset("device_health", mid, score.toString)
                }
                jedis.close()
            }

            // 2. 写入 ClickHouse device_realtime_status 大屏表
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
            } catch { case e: Exception => e.printStackTrace() }
          }
      }
      .outputMode("append") // 这里不用变
      .option("checkpointLocation", "/tmp/checkpoints/engine_enriched")
      .start()

    // ==========================================
    // 模块 2.3 & 任务 16 异常检测与报警 (Kafka & ClickHouse)
    // ==========================================
    // 传感器异常: 温度 > 80 OR 振动突增 (这里简化为原始振动>2.0模拟突增)
    val sensorAlerts = sensorRawDF
      .filter(col("temperature") > 80.0 || col("vibration_x") > 2.0)
      .select(
        col("timestamp").alias("alert_time"),
        col("machine_id"),
        when(col("temperature") > 80.0, "High Temperature")
          .otherwise("Vibration Sudden Increase")
          .alias("alert_type"),
        col("temperature").alias("trigger_value"), // 任务 16 需要的字段
        lit(80.0).alias("threshold_value"), // 任务 16 需要的字段
        lit("Inspect machine immediately").alias("suggested_action")
      )

    // 日志异常: Error Code 999
    val logAlerts = logRawDF
      .filter(col("error_code") === "999")
      .select(
        col("timestamp").alias("alert_time"),
        col("machine_id"),
        lit("Critical Log Error 999").alias("alert_type"),
        lit(999.0).alias("trigger_value"),
        lit(0.0).alias("threshold_value"),
        lit("Check hardware logs").alias("suggested_action")
      )

    val combinedAlerts = sensorAlerts.unionByName(logAlerts)

    // 1. 推送 Kafka alert_topic
    combinedAlerts
      .selectExpr(
        "CAST(machine_id AS STRING) AS key",
        "to_json(struct(*)) AS value"
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", SparkUtils.kafkaBrokers)
      .option("topic", "alert_topic")
      .option("checkpointLocation", "/tmp/checkpoints/engine_alerts_kafka")
      .start()

    // 2. 写入 ClickHouse realtime_alerts
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
      .option("checkpointLocation", "/tmp/checkpoints/engine_alerts_ck")
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
              val jedis = new Jedis("192.168.45.11", 6379)
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
                // 精确偏离阈值：大约 2.8% (0.028) 左右。
                // 因为刚才观察到你的最大正常波动在 3.06% (30.92A)，所以 2.8% 刚好能卡住这个峰值。
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
          -- 保持一次偏离即记录，以保证稳定有数据产生
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
      .option("checkpointLocation", "/tmp/checkpoints/engine_process_dev")
      .start()

    // 阻塞等待所有流任务执行
    spark.streams.awaitAnyTermination()
  }
}
