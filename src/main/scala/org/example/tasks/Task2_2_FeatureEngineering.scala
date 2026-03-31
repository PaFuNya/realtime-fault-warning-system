package org.example.tasks

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/** 模块二：实时数据处理引擎 2.1 填补短时缺失值（线性插值） 2.2 实时特征计算 (状态机识别、温度上升斜率、振动突增率)
  */
object Task2_2_FeatureEngineering {

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getSparkSession("Task2_2_FeatureEngineering")

    val sensorRawDF = SparkUtils
      .getSensorRawStream(spark)
      .withWatermark("timestamp", "5 seconds")

    // 在 foreachBatch 中使用纯 SQL/DataFrame 方式实现状态特征计算，避免手写复杂的 flatMapGroupsWithState
    sensorRawDF.writeStream
      .foreachBatch {
        (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>
          // 定义按设备分区、按时间排序的窗口，用来获取“上一条数据” (lag)
          val windowSpec = Window.partitionBy("machine_id").orderBy("ts")

          val enrichedDF = batchDF
            // 1. 获取上一条数据的 ts, temp, vib_x
            .withColumn("prev_ts", lag("ts", 1).over(windowSpec))
            .withColumn("prev_temp", lag("temperature", 1).over(windowSpec))
            .withColumn("prev_vib_x", lag("vibration_x", 1).over(windowSpec))

            // 2. 2.1 缺失值填补 (如果当前为空，用上一条填补)
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

            // 3. 2.2 计算时间差(秒)、温度斜率、振动突增率
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

            // 4. 2.2 状态机识别 (根据转速)
            .withColumn(
              "status",
              when(col("speed") > 1000, "稳定运行")
                .when(col("speed") < 100, "异常停机")
                .otherwise("启动中")
            )

            // 5. 健康分扣减模拟
            .withColumn(
              "health_score",
              when(col("vib_increase_ratio") > 0.2, 95).otherwise(100)
            )

            // 剔除中间辅助列，保留干净的特征结果
            .drop("prev_ts", "prev_temp", "prev_vib_x", "time_diff")

          // 打印这一批次计算出的特征结果
          enrichedDF.show(truncate = false)

      }
      .start()

    spark.streams.awaitAnyTermination()
  }
}
