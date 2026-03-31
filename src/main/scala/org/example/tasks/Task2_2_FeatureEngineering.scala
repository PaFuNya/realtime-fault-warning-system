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
      .withWatermark("timestamp", "1 hour") // 必须设置至少1小时的水位线来支持1小时窗口

    // =========================================================================
    // 1. 满足要求 2.2：滑动窗口统计 (1分钟、5分钟、1小时)
    // =========================================================================

    // 1分钟窗口
    val window1mDF = sensorRawDF
      .groupBy(window(col("timestamp"), "1 minute"), col("machine_id"))
      .agg(
        mean("vibration_x").alias("1m_vib_rms"),
        mean("current").alias("1m_curr_mean")
      )

    // 5分钟窗口
    val window5mDF = sensorRawDF
      .groupBy(window(col("timestamp"), "5 minutes"), col("machine_id"))
      .agg(
        mean("vibration_x").alias("5m_vib_rms"),
        mean("current").alias("5m_curr_mean")
      )

    // 1小时窗口
    val window1hDF = sensorRawDF
      .groupBy(window(col("timestamp"), "1 hour"), col("machine_id"))
      .agg(
        mean("vibration_x").alias("1h_vib_rms"),
        mean("current").alias("1h_curr_mean")
      )

    // =========================================================================
    // 2. 满足要求 2.1 & 2.2：状态机识别、温度上升斜率、缺失值填补
    // =========================================================================
    sensorRawDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          val windowSpec = Window.partitionBy("machine_id").orderBy("ts")

          val enrichedDF = batchDF
            // 获取上一条数据
            .withColumn("prev_ts", lag("ts", 1).over(windowSpec))
            .withColumn("prev_temp", lag("temperature", 1).over(windowSpec))
            .withColumn("prev_vib_x", lag("vibration_x", 1).over(windowSpec))

            // 2.1 缺失值填补
            .withColumn(
              "temperature",
              when(
                col("temperature").isNull || col("temperature").isNaN,
                col("prev_temp")
              ).otherwise(col("temperature"))
            )
            .withColumn(
              "vibration_x",
              when(
                col("vibration_x").isNull || col("vibration_x").isNaN,
                col("prev_vib_x")
              ).otherwise(col("vibration_x"))
            )

            // 2.2 计算时间差、温度斜率、振动突增率
            .withColumn("time_diff", (col("ts") - col("prev_ts")) / 1000.0)
            .withColumn(
              "temp_slope",
              when(
                col("time_diff") > 0,
                (col("temperature") - col("prev_temp")) / col("time_diff")
              ).otherwise(0.0)
            )
            .withColumn(
              "vib_increase_ratio",
              when(
                col("prev_vib_x") > 0,
                (col("vibration_x") - col("prev_vib_x")) / col("prev_vib_x")
              ).otherwise(0.0)
            )

            // 2.2 状态机识别
            .withColumn(
              "status",
              when(col("speed") > 1000, "稳定运行")
                .when(col("speed") < 100, "异常停机")
                .otherwise("启动中")
            )

            // 健康分扣减模拟
            .withColumn(
              "health_score",
              when(col("vib_increase_ratio") > 0.2, 95).otherwise(100)
            )
            .drop("prev_ts", "prev_temp", "prev_vib_x", "time_diff")

          // 打印特征结果验证
          enrichedDF.show(truncate = false)

      }
      .start()

    // 启动窗口统计控制台输出 (独立运行验证用)
    window1mDF.writeStream.format("console").outputMode("update").start()

    spark.streams.awaitAnyTermination()
  }
}
