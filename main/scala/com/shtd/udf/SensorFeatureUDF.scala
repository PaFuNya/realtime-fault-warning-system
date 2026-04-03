package com.shtd.udf

import org.apache.commons.math3.stat.descriptive.moment.{Kurtosis, Variance}
import org.apache.commons.math3.transform.{DftNormalization,FastFourierTransformer, TransformType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types._

import scala.collection.mutable.WrappedArray
import scala.util.Try

/**
 * 特征向量结果类 (用于 UDF 内部返回)
 */
case class FeatureVector(
                          mean: Double,
                          variance: Double,
                          kurtosis: Double,
                          fft_peak: Double
                        )

/**
 * 核心 UDF 类
 * 输入: WrappedArray[String] (对应数据库中 Temperature 字段的字符串数组)
 * 输出: Row (包含计算好的特征)
 */
object SensorFeatureUDF extends UDF1[WrappedArray[String], Row] {

  // 1. 定义 UDF 返回的 Schema (结构)
  val schema: StructType = StructType(Seq(
    StructField("mean", DoubleType, nullable = true),
    StructField("variance", DoubleType, nullable = true),
    StructField("kurtosis", DoubleType, nullable = true),
    StructField("fft_peak", DoubleType, nullable = true)
  ))

  /**
   * 核心计算方法
   */
  override def call(temperatureStrings: WrappedArray[String]): Row = {

    // --- 步骤 A: 数据清洗与转换 (String -> Double) ---
    if (temperatureStrings == null || temperatureStrings.isEmpty) {
      return Row(null, null, null, null)
    }

    // 将字符串数组转为 Double 数组，过滤掉无法转换的脏数据 (如 "N/A", "")
    val doubles = temperatureStrings
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap(s => Try(s.toDouble).toOption) // 安全转换
      .toArray

    // 数据量太少无法计算 FFT 和统计特征
    if (doubles.length < 4) {
      return Row(null, null, null, null)
    }

    // --- 步骤 B: 时域特征计算 ---

    // 1. 均值
    val mean = doubles.sum / doubles.length

    // 2. 方差 (使用 Apache Commons Math)
    val variance = new Variance().evaluate(doubles)

    // 3. 峭度 (Kurtosis - 衡量波形的尖锐程度，故障诊断关键指标)
    val kurtosis = new Kurtosis().evaluate(doubles)

    // --- 步骤 C: 频域特征计算 (FFT) ---
    var fftPeak = 0.0
    try {
      val transformer = new FastFourierTransformer(DftNormalization.STANDARD)

      // FFT 要求数据长度必须是 2 的幂 (2^n)
      // 如果长度不是 2 的幂，需要补零 (Padding)
      val paddedLength = Integer.highestOneBit(doubles.length) * 2
      val paddedData = java.util.Arrays.copyOf(doubles, paddedLength)

      // 执行快速傅里叶变换
      val complexResults = transformer.transform(paddedData, TransformType.FORWARD)

      // 计算频谱幅值，并找到最大值 (主频峰值)
      // 忽略第一个点 (直流分量/0Hz)，从索引 1 开始找最大值
      val magnitudes = complexResults.drop(1).map(c => c.abs)
      if (magnitudes.nonEmpty) {
        fftPeak = magnitudes.max
      }
    } catch {
      case e: Exception =>
        // 生产环境建议加上日志记录
        println(s"FFT Calculation Error: ${e.getMessage}")
        fftPeak = 0.0
    }

    // --- 步骤 D: 返回结果 ---
    Row(mean, variance, kurtosis, fftPeak)
  }
}