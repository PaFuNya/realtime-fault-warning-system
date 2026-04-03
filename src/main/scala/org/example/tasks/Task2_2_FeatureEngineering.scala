package org.example.tasks

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/** 模块二：实时数据处理引擎 2.1 填补短时缺失值（线性插值） 2.2 实时特征计算 (滑动窗口统计
  * 1m/5m/1h、状态机识别、温度上升斜率、振动突增率)
  */
object Task2_2_FeatureEngineering {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession("Task2_2_FeatureEngineering")

    val sensorRawDF = SparkUtils
      .getSensorRawStream(spark)
      .withWatermark("timestamp", "5 seconds")

    // 读取 ChangeRecord 流
    val changeRecordDF = SparkUtils
      .getChangeRecordStream(spark)
      .withWatermark("start_time", "5 seconds")

    // ==========================================
    // 1. 滑动窗口统计 (1m, 5m, 1h)
    // ==========================================
    // 1分钟窗口
    val window1mDF = sensorRawDF
      .groupBy(window(col("timestamp"), "1 minute"), col("machine_id"))
      .agg(
        // 振动 RMS (Root Mean Square) 近似计算: 假设均方根可以通过平方和均值开根号得到，这里简化为求标准差或均方
        sqrt(
          mean(
            col("vibration_x") * col("vibration_x") + col("vibration_y") * col(
              "vibration_y"
            ) + col("vibration_z") * col("vibration_z")
          )
        ).alias("1m_vib_rms"),
        stddev("current").alias("1m_curr_stddev"), // 电流波动率用标准差表示
        // 温度上升斜率: (最后温度 - 最早温度) / 时间跨度，在无状态窗口中简化为 max - min
        (max("temperature") - min("temperature")).alias("1m_temp_slope")
      )

    // 5分钟窗口
    val window5mDF = sensorRawDF
      .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("machine_id")
      )
      .agg(
        sqrt(
          mean(
            col("vibration_x") * col("vibration_x") + col("vibration_y") * col(
              "vibration_y"
            ) + col("vibration_z") * col("vibration_z")
          )
        ).alias("5m_vib_rms"),
        stddev("current").alias("5m_curr_stddev"),
        (max("temperature") - min("temperature")).alias("5m_temp_slope")
      )

    // 1小时窗口
    val window1hDF = sensorRawDF
      .groupBy(
        window(col("timestamp"), "1 hour", "5 minutes"),
        col("machine_id")
      )
      .agg(
        sqrt(
          mean(
            col("vibration_x") * col("vibration_x") + col("vibration_y") * col(
              "vibration_y"
            ) + col("vibration_z") * col("vibration_z")
          )
        ).alias("1h_vib_rms"),
        stddev("current").alias("1h_curr_stddev"),
        (max("temperature") - min("temperature")).alias("1h_temp_slope")
      )

    // ==========================================
    // 2. 状态机识别与双流 Join
    // ==========================================
    // 将 Sensor 流与 ChangeRecord 流进行 Stream-Stream Join
    // 条件: machine_id 相同，且 Sensor 的 timestamp 在 ChangeRecord 的发生时间段内或之后不久
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

    // ==========================================
    // 3. 特征计算与预测过滤 (foreachBatch)
    // ==========================================
    val query = joinedDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          if (!batchDF.isEmpty) {
            // 利用 Spark SQL 的 Window 函数计算上一条数据，进行插值和差值计算
            val windowSpec = Window
              .partitionBy("machine_id")
              .orderBy("timestamp")

            val enrichedDF = batchDF
              // 结合 ChangeRecord 的状态和当前数值判断最终状态
              .withColumn(
                "machine_status",
                when(col("status") === "预警", lit("异常停机"))
                  .when(col("speed") < 1000, lit("启动中"))
                  .otherwise(lit("稳定运行"))
              )
              .withColumn(
                "prev_ts",
                lag("timestamp", 1).over(windowSpec)
              )
              .withColumn(
                "prev_temp",
                lag("temperature", 1).over(windowSpec)
              )
              .withColumn(
                "prev_vib_x",
                lag("vibration_x", 1).over(windowSpec)
              )
              // 计算时间差 (秒)
              .withColumn(
                "time_diff",
                unix_timestamp(col("timestamp")) - unix_timestamp(
                  col("prev_ts")
                )
              )
              // 计算温度斜率 (单位时间内温度变化)
              .withColumn(
                "temp_slope",
                when(
                  col("time_diff") > 0,
                  (col("temperature") - col("prev_temp")) / col(
                    "time_diff"
                  )
                )
                  .otherwise(0.0)
              )
              // 计算振动突增率
              .withColumn(
                "vib_surge_rate",
                when(
                  col("prev_vib_x") > 0,
                  (col("vibration_x") - col("prev_vib_x")) / col(
                    "prev_vib_x"
                  )
                )
                  .otherwise(0.0)
              )
              // **仅在“稳定运行”状态下进行 RUL (剩余寿命) 预测计算**
              .withColumn(
                "rul_prediction",
                when(
                  col("machine_status") === "稳定运行",
                  // 这里写你的预测逻辑模型，示例中用一个简单公式模拟
                  lit(100.0) - (col("temperature") * 0.1) - (col(
                    "vibration_x"
                  ) * 5.0)
                )
                  .otherwise(lit(null)) // 启动中或异常停机不预测，避免误报
              )

            enrichedDF.show(truncate = false)
          }
      }
      .outputMode("append")
      .start()

    // 启动窗口统计控制台输出 (独立运行验证用)
    window1mDF.writeStream.format("console").outputMode("update").start()

    spark.streams.awaitAnyTermination()
  }
}
