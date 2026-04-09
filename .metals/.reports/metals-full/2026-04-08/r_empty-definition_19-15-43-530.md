error id: file:///D:/Desktop/Match/MATCH/OverMatch/src/test/java/testUtils.scala:cou.
file:///D:/Desktop/Match/MATCH/OverMatch/src/test/java/testUtils.scala
empty definition using pc, found symbol in pc: cou.
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -acc/cou.
	 -acc/cou#
	 -acc/cou().
	 -scala/Predef.acc.cou.
	 -scala/Predef.acc.cou#
	 -scala/Predef.acc.cou().
offset: 317
uri: file:///D:/Desktop/Match/MATCH/OverMatch/src/test/java/testUtils.scala
text:
```scala
package org.example.tasks.utils

import org.example.tasks.FlinkRulInference.{
  HighFreqAccumulator,
  HighFreqSensor,
  TimeSeriesFeatures
}

object FeatureUtils {

  /** 逐步聚合更新特征统计量
    */
  def accumulateFeatures(
      value: HighFreqSensor,
      acc: HighFreqAccumulator
  ): HighFreqAccumulator = {
    acc.cou@@
  }

  /** 合并两个特征统计量累加器
    */
  def mergeAccumulators(
      a: HighFreqAccumulator,
      b: HighFreqAccumulator
  ): HighFreqAccumulator = {
    a.count += b.count
    a.sumTemp += b.sumTemp
    a.sumTempSq += b.sumTempSq
    a.sumTempCubic += b.sumTempCubic
    a.sumTempQuad += b.sumTempQuad
    a.sumLoad += b.sumLoad
    a.maxLoad = math.max(a.maxLoad, b.maxLoad)
    a.sumFeedRate += b.sumFeedRate
    a.runningCount += b.runningCount
    a.maxVibX = math.max(a.maxVibX, b.maxVibX)
    a.vibXSumSq += b.vibXSumSq
    a
  }

  /** 根据累计状态计算出最终的 7 维时序特征结果
    */
  def calculateTimeSeriesFeatures(
      acc: HighFreqAccumulator
  ): TimeSeriesFeatures = {
    val n = acc.count.toDouble
    if (n == 0) {
      return TimeSeriesFeatures(0, 0, 0, 0, 0, 0, 0, 0)
    }

    // ---- 1. var_temp: 总体方差 (Population Variance) ----
    val meanTemp = acc.sumTemp / n
    val meanTempSq = acc.sumTempSq / n
    val variance = meanTempSq - meanTemp * meanTemp

    // ---- 2. kurtosis_temp: 峰度 ( excess kurtosis ) ----
    val std2 = variance
    val kurtosis = if (std2 > 1e-9) {
      val mean4 = acc.sumTempQuad / n
      val meanCubed = acc.sumTempCubic / n
      val central4th = mean4 - 4.0 * meanCubed * meanTemp +
        6.0 * meanTempSq * meanTemp * meanTemp -
        3.0 * math.pow(meanTemp, 4)
      central4th / (std2 * std2) - 3.0
    } else 0.0

    // ---- 3. fft_peak: 振动频域峰值 (代理指标) ----
    val vibRms = math.sqrt(acc.vibXSumSq / n)
    val peakFactor = if (vibRms > 1e-6) acc.maxVibX / vibRms else 0.0
    val fftPeak = vibRms * peakFactor

    // ---- 4. avg_spindle_load ----
    val avgLoad = acc.sumLoad / n

    // ---- 5. max_spindle_load ----
    val maxLoad = acc.maxLoad

    // ---- 6. running_ratio ----
    val runRatio = acc.runningCount / n

    // ---- 7. avg_feed_rate ----
    val avgFeed = acc.sumFeedRate / n

    TimeSeriesFeatures(
      machineId = 0, // 由上游 keyBy 传入，这里填0，后续从key获取
      var_temp = math.round(variance * 100.0) / 100.0,
      kurtosis_temp = math.round(kurtosis * 100.0) / 100.0,
      fft_peak = math.round(fftPeak * 1000.0) / 1000.0,
      avg_spindle_load = math.round(avgLoad * 10.0) / 10.0,
      max_spindle_load = math.round(maxLoad * 10.0) / 10.0,
      running_ratio = math.round(runRatio * 10000.0) / 10000.0,
      avg_feed_rate = math.round(avgFeed)
    )
  }

  /** 将计算好的高频时序特征填充到报警记录中 */
  def fillAlarmRecordWithFeatures(
      record: org.example.tasks.FlinkRulInference.AlarmRecord,
      features: TimeSeriesFeatures
  ): org.example.tasks.FlinkRulInference.AlarmRecord = {
    record.copy(
      var_temp = features.var_temp,
      kurtosis_temp = features.kurtosis_temp,
      fft_peak = features.fft_peak,
      avg_spindle_load_win = features.avg_spindle_load,
      max_spindle_load_win = features.max_spindle_load,
      running_ratio = features.running_ratio,
      avg_feed_rate = features.avg_feed_rate
    )
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: cou.