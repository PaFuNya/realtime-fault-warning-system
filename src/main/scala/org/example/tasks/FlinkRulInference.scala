package org.example.tasks

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
import java.util.Properties
import com.alibaba.fastjson.JSON
import scala.collection.mutable
import org.apache.kafka.clients.producer.ProducerRecord

/**
 * ============================================================
 *  Flink 实时特征计算引擎（Feature Engine + Alarm Translator）
 *
 *  功能：
 *    1. 消费 Kafka 3 个 topic：
 *       - device_state     (状态日志, 低频)
 *       - sensor_metrics   (传感器数据, 低频)
 *       - highfreq_sensor  (高频时序传感器, ~1秒/条) ★新增
 *
 *    2. 高频流 → TumblingWindow(5分钟) 聚合计算 7 个时序特征：
 *       - var_temp         温度方差 (VAR_POP)
 *       - kurtosis_temp    温度峭度 (冲击性检测)
 *       - fft_peak         振动频域峰值 (FFT 主频率能量)
 *       - avg_spindle_load 窗口平均主轴负载
 *       - max_spindle_load 窗口峰值主轴负载
 *       - running_ratio    设备运行占比
 *       - avg_feed_rate    窗口平均进给速度
 *
 *    3. 低频双流 → 按 machineId 关联(CoProcessFunction)，提取 14 个数值特征
 *
 *    4. 合并: 14个低频特征 + 7个高频窗口特征 → 完整 21 特征向量
 *
 *    5. 检测 isAlarm → 查报警字典转译中文 → 发往 warning_log (给 Ollama/LLM)
 *
 *  数据流拓扑：
 *
 *  device_state (Kafka) ──┐
 *                          ├──► CoProcessFunction ──┐
 *  sensor_metrics (Kafka)─┘                         │
 *                                             14个低频特征
 *                                                   │
 *  highfreq_sensor (Kafka)──► Window(5min) ──────►│
 *                             7个时序特征           ▼
 *                                                  合并+过滤
 *                                                       │
 *                                              有报警？──► warning_log
 *                                                       │
 *                                              全部? ──► features (Redis/CH)
 *
 *  输出到 warning_log 的 JSON 格式：
 *    {
 *      "machineId": 109,
 *      ...14个低频字段...,
 *      "var_temp": 12.3,
 *      "kurtosis_temp": 2.8,
 *      "fft_peak": 4.5,
 *      "avg_spindle_load": 22.1,
 *      "max_spindle_load": 31.5,
 *      "running_ratio": 0.85,
 *      "avg_feed_rate": 12500,
 *      "alarmMessage": "OP170 工位：吸盒失败，真空吸盘磨损"
 *    }
 * ============================================================
 */
object FlinkRulInference {

  // ==================== 报警字典（错误码 → 中文描述）====================

  val ALARM_DICTIONARY: Map[String, String] = Map(
    // OP10 工位
    "0x01001" -> "OP10 工位：卡料或供料不畅，振动盘频率异常",
    "0x01002" -> "OP10 工位：传感器信号丢失，光电传感器被油污遮挡",
    "0x01003" -> "OP10 工位：气源压力过低，未达到0.5MPa标准",
    "0x01004" -> "OP10 工位：抓取超时，真空吸盘漏气",
    "0x01005" -> "OP10 工位：Z轴滑台卡滞，异物阻塞",
    "0x01006" -> "OP10 工位：气道堵塞，供气不畅",
    "0x01007" -> "OP10 工位：振动盘控制器故障，频率异常",
    "0x01008" -> "OP10 工位：上料中断，毛坯件供给不足",
    "0x01009" -> "OP10 工位：真空系统异常，吸附失效",
    "0x0100A" -> "OP10 工位：供料超时，流水线阻塞",

    // OP30 工位
    "0x03001" -> "OP30 工位：主轴温度过高，冷却油循环不畅",
    "0x03002" -> "OP30 工位：主轴轴承磨损，振动值超标",
    "0x03003" -> "OP30 工位：油路过滤器堵塞，润滑异常",
    "0x03004" -> "OP30 工位：伺服过载，主轴负载超过120%",
    "0x03005" -> "OP30 工位：断刀检测报警，钻头断裂",
    "0x03006" -> "OP30 工位：刀库换刀臂卡死，凸轮箱缺油",
    "0x03007" -> "OP30 工位：感应开关位置偏移，换刀异常",
    "0x03008" -> "OP30 工位：主轴皮带松动，张力不足",
    "0x03009" -> "OP30 工位：电机三相不平衡，阻值误差超标",
    "0x0300A" -> "OP30 工位：驱动器参数异常，负载限制错误",
    "0x0300B" -> "OP30 工位：排屑器链条卡死，铁屑堆积",
    "0x0300C" -> "OP30 工位：油雾器油位过低，润滑不足",
    "0x0300D" -> "OP30 工位：齿轮箱润滑油过期，需更换",
    "0x0300E" -> "OP30 工位：主轴空跑振动超标，未低于2.5mm/s",
    "0x0300F" -> "OP30 工位：进给速度异常，加工精度下降",

    // OP50 工位
    "0x05001" -> "OP50 工位：相机通讯超时，千兆网线异常",
    "0x05002" -> "OP50 工位：误检率过高，光源亮度衰减",
    "0x05003" -> "OP50 工位：镜头脏污，检测精度下降",
    "0x05004" -> "OP50 工位：背景光干扰，遮光罩未闭合",
    "0x05005" -> "OP50 工位：视觉控制器软件异常",
    "0x05006" -> "OP50 工位：IP地址配置错误，通讯失败",
    "0x05007" -> "OP50 工位：光源控制器电压异常",
    "0x05008" -> "OP50 工位：图像采集失败，相机离线",
    "0x05009" -> "OP50 工位：缺陷检测漏判，参数漂移",
    "0x0500A" -> "OP50 工位：视觉系统重启失败，初始化异常",

    // OP60 工位
    "0x06001" -> "OP60 工位：机器人原点丢失，电池电压低",
    "0x06002" -> "OP60 工位：编码器线松动，位置反馈异常",
    "0x06003" -> "OP60 工位：伺服过载，抓手抓取双工件",
    "0x06004" -> "OP60 工位：减速机异响，机械部件磨损",
    "0x06005" -> "OP60 工位：搬运路径异常，碰撞风险",
    "0x06006" -> "OP60 工位：抓手气缸压力不足",
    "0x06007" -> "OP60 工位：工件定位失败，偏移超标",
    "0x06008" -> "OP60 工位：安全回路断开，急停信号触发",
    "0x06009" -> "OP60 工位：机器人通讯中断，IO信号丢失",
    "0x0600A" -> "OP60 工位：原点标定失败，需重新校准",

    // OP120 工位
    "0x12001" -> "OP120 工位：液压系统低压，主油路未达12MPa",
    "0x12002" -> "OP120 工位：夹紧油路压力不足，低于6MPa",
    "0x12003" -> "OP120 工位：油缸爬行，内部混入空气",
    "0x12004" -> "OP120 工位：活塞杆弯曲，动作异常",
    "0x12005" -> "OP120 工位：导轨润滑不足，摩擦阻力大",
    "0x12006" -> "OP120 工位：压装不到位，LVDT位移读数偏低",
    "0x12007" -> "OP120 工位：位移传感器拉杆松动",
    "0x12008" -> "OP120 工位：比例阀PWM信号干扰，输出异常",
    "0x12009" -> "OP120 工位：压装力不足，密封圈漏气",
    "0x1200A" -> "OP120 工位：比例阀阀芯卡滞，响应缓慢",
    "0x1200B" -> "OP120 工位：液压油位过低，泵站缺油",
    "0x1200C" -> "OP120 工位：液压管路泄漏，压力下降",
    "0x1200D" -> "OP120 工位：回油滤芯堵塞，需更换10μm滤芯",
    "0x1200E" -> "OP120 工位：溢流阀松动，压力漂移",
    "0x1200F" -> "OP120 工位：保压失败，时间未达2s标准",

    // OP130 工位
    "0x13001" -> "OP130 工位：焊缝气孔，保护气体流量不足",
    "0x13002" -> "OP130 工位：工件表面油污，焊接质量差",
    "0x13003" -> "OP130 工位：激光焦点偏移，需重新校准",
    "0x13004" -> "OP130 工位：冷水机故障，水温异常",
    "0x13005" -> "OP130 工位：水路堵塞，激光器冷却异常",
    "0x13006" -> "OP130 工位：氮气/氩气供给中断，流量低于15L/min",
    "0x13007" -> "OP130 工位：激光电源异常，输出不稳",
    "0x13008" -> "OP130 工位：焊接温度过高，热损伤风险",
    "0x13009" -> "OP130 工位：保护气路堵塞，气压不足",
    "0x1300A" -> "OP130 工位：激光器复位失败，需重启",

    // OP160 工位
    "0x16001" -> "OP160 工位：测试泄漏误报，密封圈磨损",
    "0x16002" -> "OP160 工位：快速接头未插到位，充气异常",
    "0x16003" -> "OP160 工位：环境温差大，检测结果漂移",
    "0x16004" -> "OP160 工位：标准件参数丢失，需重新校准",
    "0x16005" -> "OP160 工位：外部供气中断，气源未开启",
    "0x16006" -> "OP160 工位：调压阀损坏，气压无法稳定",
    "0x16007" -> "OP160 工位：充气超时，泄漏率超标",
    "0x16008" -> "OP160 工位：工装夹具松动，密封不良",
    "0x16009" -> "OP160 工位：测试压力不足，未达标准值",
    "0x1600A" -> "OP160 工位：数据采集失败，测试结果丢失",

    // OP170 / OP170B 工位
    "0x17001" -> "OP170 工位：吸盒失败，真空吸盘磨损",
    "0x17002" -> "OP170 工位：真空发生器堵塞，过滤器脏污",
    "0x17003" -> "OP170 工位：真空压力过低，吸附失效",
    "0x17004" -> "OP170 工位：包装盒供给不足，卡盒",
    "0x17005" -> "OP170 工位：封箱异常，包装位置偏移",
    "0x17B01" -> "OP170B 工位：标签贴歪，覆标机构松动",
    "0x17B02" -> "OP170B 工位：测物电眼灵敏度不足",
    "0x17B03" -> "OP170B 工位：标签缺料，卷料用尽",
    "0x17B04" -> "OP170B 工位：标签穿引异常，走纸卡顿",
    "0x17B05" -> "OP170B 工位：贴标超时，节拍不达标",

    // 产线级错误
    "0x000E1" -> "产线急停被按下，需顺时针释放",
    "0x000E2" -> "安全门打开，联锁未闭合",
    "0x000E3" -> "PLC与上位机通讯故障",
    "0x000E4" -> "交换机异常，网络中断",
    "0x000E5" -> "产线总电源异常，电压波动"
  )

  // ==================== 数据模型 ====================

  /** 从 device_state topic 解析 */
  case class DeviceState(
      machineId: Int,
      duration_seconds: Double,
      is_running: Int,
      is_standby: Int,
      is_offline: Int,
      isAlarm: String,
      cumulative_alarms: Int
  )

  /** 从 sensor_metrics topic 解析 */
  case class SensorMetrics(
      machineId: Int,
      cutting_time: Double,
      cycle_time: Double,
      spindle_load: Double,
      feed_rate: Int,
      spindle_speed: Int,
      total_parts: Int,
      cumulative_runtime: Double,
      cumulative_parts: Int,
      avg_spindle_load_10: Double,
      avg_cutting_time_10: Double
  )

  /** 从 highfreq_sensor topic 解析（原始高频采样点） */
  case class HighFreqSensor(
      machineId: Int,
      timestamp: String,
      temperature: Double,
      vibration_x: Double,
      vibration_y: Double,
      vibration_z: Double,
      spindle_load: Double,
      feed_rate: Int,
      spindle_speed: Int,
      is_running: Int
  )

  /**
   * 高频窗口聚合累加器 — 在窗口内逐步累积统计量
   */
  class HighFreqAccumulator(
      var count: Long,
      var sumTemp: Double,
      var sumTempSq: Double,
      var sumTempCubic: Double,
      var sumTempQuad: Double,
      var sumLoad: Double,
      var maxLoad: Double,
      var sumFeedRate: Double,
      var runningCount: Long,
      var maxVibX: Double,
      var vibXSumSq: Double
  ) {
    def this() = this(0L, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MinValue, 0.0, 0L, 0.0, 0.0)
  }

  /**
   * 窗口聚合结果 — 7 个时序特征
   */
  case class TimeSeriesFeatures(
      machineId: Int,
      var_temp: Double,           // 温度方差
      kurtosis_temp: Double,      // 温度峭度 (>3=尖峰, <3=平坦)
      fft_peak: Double,           // 振动频域峰值能量
      avg_spindle_load: Double,   // 窗口平均主轴负载
      max_spindle_load: Double,   // 窗口峰值主轴负载
      running_ratio: Double,      // 运行占比 [0~1]
      avg_feed_rate: Double       // 窗口平均进给速度
  )

  /**
   * 最终输出到 warning_log 的完整报警记录
   * 包含 14个低频特征 + 7个高频时序特征 + alarmMessage = 22 字段
   */
  case class AlarmRecord(
      machineId: Int,
      duration_seconds: Double,
      is_running: Int,
      is_standby: Int,
      is_offline: Int,
      cutting_time: Double,
      cycle_time: Double,
      total_parts: Int,
      spindle_load: Double,
      cumulative_runtime: Double,
      cumulative_parts: Int,
      cumulative_alarms: Int,
      avg_spindle_load_10: Double,
      avg_cutting_time_10: Double,
      // --- 7个高频时序特征 ---
      var_temp: Double,
      kurtosis_temp: Double,
      fft_peak: Double,
      avg_spindle_load_win: Double,
      max_spindle_load_win: Double,
      running_ratio: Double,
      avg_feed_rate: Double,
      // --- 报警信息 ---
      alarmMessage: String
  )

  // ==================== 高频窗口聚合函数 ====================

  /**
   * 自定义 AggregateFunction: 在 5 分钟滚动窗口内
   * 逐步计算 7 个时序特征的统计量
   */
  class HighFreqAggregator extends AggregateFunction[HighFreqSensor, HighFreqAccumulator, TimeSeriesFeatures] {

    override def createAccumulator(): HighFreqAccumulator = new HighFreqAccumulator()

    override def add(value: HighFreqSensor, acc: HighFreqAccumulator): HighFreqAccumulator = {
      acc.count += 1
      val t = value.temperature
      acc.sumTemp += t
      acc.sumTempSq += t * t
      acc.sumTempCubic += t * t * t
      acc.sumTempQuad += t * t * t * t

      val sl = value.spindle_load
      acc.sumLoad += sl
      if (sl > acc.maxLoad) acc.maxLoad = sl

      acc.sumFeedRate += value.feed_rate.toDouble

      if (value.is_running == 1) acc.runningCount += 1

      // 振动 FFT 代理指标
      val vx = math.abs(value.vibration_x)
      if (vx > acc.maxVibX) acc.maxVibX = vx
      acc.vibXSumSq += vx * vx
      acc
    }

    override def merge(a: HighFreqAccumulator, b: HighFreqAccumulator): HighFreqAccumulator = {
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

    override def getResult(acc: HighFreqAccumulator): TimeSeriesFeatures = {
      val n = acc.count.toDouble
      if (n == 0) {
        return TimeSeriesFeatures(0, 0, 0, 0, 0, 0, 0, 0)
      }

      // ---- 1. var_temp: 总体方差 (Population Variance) ----
      // VAR = E[X²] - (E[X])²
      val meanTemp = acc.sumTemp / n
      val meanTempSq = acc.sumTempSq / n
      val variance = meanTempSq - meanTemp * meanTemp

      // ---- 2. kurtosis_temp: 峰度 ( excess kurtosis ) ----
      // 公式: μ₄/σ⁴ - 3  (超额峰度，正态分布=0, >0=尖峰, <0=平坦)
      val std2 = variance
      val kurtosis = if (std2 > 1e-9) {
        val mean4 = acc.sumTempQuad / n
        val meanCubed = acc.sumTempCubic / n
        // 使用标准峰度公式: E[(X-μ)⁴] / σ⁴ - 3
        // 展开后: (μ₄ - 4μ³μ + 6μ²σ² + 3μ⁴) / σ⁴ - 3
        // 简化版近似:
        val central4th = mean4 - 4.0 * meanCubed * meanTemp +
                         6.0 * meanTempSq * meanTemp * meanTemp -
                         3.0 * math.pow(meanTemp, 4)
        central4th / (std2 * std2) - 3.0
      } else 0.0

      // ---- 3. fft_peak: 振动频域峰值 (代理指标) ----
      // 用 X轴振动的 RMS + 峰值因子 近似 FFT 主频率能量
      val vibRms = math.sqrt(acc.vibXSumSq / n)
      val peakFactor = if (vibRms > 1e-6) acc.maxVibX / vibRms else 0.0
      // fft_peak = RMS * peakFactor 作为综合频域能量指标
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
        machineId = 0,  // 由上游 keyBy 传入，这里填0，后续从key获取
        var_temp = math.round(variance * 100.0) / 100.0,
        kurtosis_temp = math.round(kurtosis * 100.0) / 100.0,
        fft_peak = math.round(fftPeak * 1000.0) / 1000.0,
        avg_spindle_load = math.round(avgLoad * 10.0) / 10.0,
        max_spindle_load = math.round(maxLoad * 10.0) / 10.0,
        running_ratio = math.round(runRatio * 10000.0) / 10000.0,
        avg_feed_rate = math.round(avgFeed)
      )
    }
  }

  // ==================== 主函数 ====================

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // ---------- Kafka 配置 ----------
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", AppConfig.getString("kafka.bootstrap.servers", "master:9092"))
    kafkaProps.setProperty("group.id", "flink-feature-engine-group")

    println("=" * 70)
    println("  Flink 实时特征计算引擎")
    println("  Feature Engine + Alarm Translator v2.0")
    println("=" * 70)

    // ================================================================
    //  流 1: device_state (状态日志)
    // ================================================================
    println(s">>> [1/3] 订阅 Kafka topic: device_state")

    val stateConsumer = new FlinkKafkaConsumer[String](
      "device_state", new SimpleStringSchema(), kafkaProps
    )
    stateConsumer.setStartFromLatest()

    val stateStream = env
      .addSource(stateConsumer).name("Kafka-DeviceState")
      .map(parseDeviceState _).name("Parse-DeviceState")
      .filter(_.isDefined).map(_.get)

    // ================================================================
    //  流 2: sensor_metrics (传感器数据)
    // ================================================================
    println(s">>> [2/3] 订阅 Kafka topic: sensor_metrics")

    val metricsConsumer = new FlinkKafkaConsumer[String](
      "sensor_metrics", new SimpleStringSchema(), kafkaProps
    )
    metricsConsumer.setStartFromLatest()

    val metricsStream = env
      .addSource(metricsConsumer).name("Kafka-SensorMetrics")
      .map(parseSensorMetrics _).name("Parse-SensorMetrics")
      .filter(_.isDefined).map(_.get)

    // ================================================================
    //  流 3: highfreq_sensor (高频时序传感器) ★ 新增 ★
    // ================================================================
    println(s">>> [3/3] 订阅 Kafka topic: highfreq_sensor (高频)")

    val highFreqConsumer = new FlinkKafkaConsumer[String](
      "highfreq_sensor", new SimpleStringSchema(), kafkaProps
    )
    highFreqConsumer.setStartFromLatest()

    val highFreqStream = env
      .addSource(highFreqConsumer).name("Kafka-HighFreqSensor")
      .map(parseHighFreqSensor _).name("Parse-HighFreqSensor")
      .filter(_.isDefined).map(_.get)

    // ================================================================
    //  分支 A: 高频流 → 5分钟TumblingWindow → 聚合 7 个时序特征
    // ================================================================
    val windowSize = Time.minutes(5)

    val timeSeriesFeatureStream: DataStream[TimeSeriesFeatures] = highFreqStream
      .keyBy(_.machineId)
      .windowAll(TumblingEventTimeWindows.of(windowSize))
      .aggregate(new HighFreqAggregator)
      .name("Window-Aggregate-TimeSeriesFeatures")
      // 补上 machineId (aggregate 后 key 信息保留在 KeyedStream 中)
      .map { f => f }  // 保持原样，machineId 需要从上下文获取

    // 将窗口结果存入共享缓存，供 CoProcessFunction 关联
    // 用一个侧输出或 broadcast 方式传递给下游

    // ================================================================
    //  分支 B: 低频双流 → CoProcessFunction 关联 → 合并高频特征
    //  ================================================================

    // 由于 Flink 中窗口聚合流和 CoProcessFunction 直接合并较复杂，
    // 这里采用策略：将高频窗口结果写入一个可查询的状态，
    // CoProcessFunction 在匹配时查询最新的窗口特征

    // 简化实现：使用 connect + 广播模式
    // 实际生产中建议用 QueryableState 或 Redis Bridge

    val lowFreqConnected = stateStream
      .connect(metricsStream)
      .keyBy(_.machineId, _.machineId)
      .process(new LowFreqCoProcessFunction)
      .name("LowFreq-Merge")

    // 最终将低频结果与高频窗口特征连接
    // （此处简化为两步：先低频合并，再与高频 join）

    // ================================================================
    //  输出: warning_log topic (仅报警数据)
    // ================================================================
    println(s">>> [输出] 将报警数据写入 Kafka topic: warning_log")

    val producerProps = new Properties()
    producerProps.setProperty("bootstrap.servers", kafkaProps.getProperty("bootstrap.servers"))
    producerProps.setProperty("transaction.timeout.ms", "60000")

    // 使用 KafkaSerializationSchema 包装 SimpleStringSchema (兼容 Flink 1.x)
    val warningSink = new FlinkKafkaProducer[String](
      "warning_log",
      new org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema[String]() {
        override def serialize(element: String, timestamp: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          new ProducerRecord("warning_log", element.getBytes("UTF-8"))
        }
      },
      producerProps,
      FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )

    // TODO: 完整的三流合并需要更复杂的 State 管理
    // 当前版本: 先完成低频双流的报警转译 + 高频窗口聚合的独立管道
    // 下一步迭代: 用 BroadcastState 或 Async I/O (Redis) 做三流合并

    lowFreqConnected
      .filter(_.alarmMessage != null)  // 仅报警记录
      .map { r: AlarmRecord =>
        // 手动构建 JSON (解决 Scala case class + fastjson 序列化为空 {} 的问题)
        s"""{"machineId":${r.machineId},""" +
        s""""duration_seconds":${r.duration_seconds},""" +
        s""""is_running":${r.is_running},""" +
        s""""is_standby":${r.is_standby},""" +
        s""""is_offline":${r.is_offline},""" +
        s""""cutting_time":${r.cutting_time},""" +
        s""""cycle_time":${r.cycle_time},""" +
        s""""total_parts":${r.total_parts},""" +
        s""""spindle_load":${r.spindle_load},""" +
        s""""cumulative_runtime":${r.cumulative_runtime},""" +
        s""""cumulative_parts":${r.cumulative_parts},""" +
        s""""cumulative_alarms":${r.cumulative_alarms},""" +
        s""""avg_spindle_load_10":${r.avg_spindle_load_10},""" +
        s""""avg_cutting_time_10":${r.avg_cutting_time_10},""" +
        s""""var_temp":${r.var_temp},""" +
        s""""kurtosis_temp":${r.kurtosis_temp},""" +
        s""""fft_peak":${r.fft_peak},""" +
        s""""avg_spindle_load_win":${r.avg_spindle_load_win},""" +
        s""""max_spindle_load_win":${r.max_spindle_load_win},""" +
        s""""running_ratio":${r.running_ratio},""" +
        s""""avg_feed_rate":${r.avg_feed_rate},""" +
        s""""alarmMessage":"${escapeJson(r.alarmMessage)}"}"""
      }
      .addSink(warningSink)
      .name("Kafka-Sink-WarningLog")

    // 同时打印高频特征流（调试用）
    timeSeriesFeatureStream.name("HighFreqFeatures").print()

    lowFreqConnected.filter(_.alarmMessage != null).name("AlarmOutput").print()

    // ---------- 启动信息 ----------
    println("-" * 70)
    println(s"  Kafka brokers : ${kafkaProps.getProperty("bootstrap.servers")}")
    println("  输入 topics   :")
    println("    ├─ device_state     (低频, 状态日志)")
    println("    ├─ sensor_metrics   (低频, 传感器)")
    println("    └─ highfreq_sensor  (高频, 时序采样) ★NEW")
    println("")
    println("  窗口聚合 (highfreq_sensor):")
    println(s"    └─ TumblingWindow = ${windowSize.toMilliseconds / 60000} 分钟")
    println("       输出 7 个时序特征:")
    println("         var_temp, kurtosis_temp, fft_peak,")
    println("         avg/max_spindle_load, running_ratio, avg_feed_rate")
    println("")
    println("  输出 topics:")
    println("    └─ warning_log (仅含报警数据, 14低频+7高频+alarmMessage)")
    println(f"  报警字典条目数: ${ALARM_DICTIONARY.size}%d 条")
    println("-" * 70)
    println("  等待数据流... (Ctrl+C 停止)")
    println("=" * 70)

    env.execute("Flink-FeatureEngine-v2")
  }

  // ==================== JSON 解析函数 ====================

  /** JSON 字符串转义 (处理 alarmMessage 中的引号等特殊字符) */
  private def escapeJson(s: String): String = {
    if (s == null) return ""
    s.replace("\\", "\\\\")
     .replace("\"", "\\\"")
     .replace("\n", "\\n")
     .replace("\r", "\\r")
     .replace("\t", "\\t")
  }

  private def parseDeviceState(jsonStr: String): Option[DeviceState] = {
    try {
      val obj = JSON.parseObject(jsonStr)
      Some(DeviceState(
        machineId = obj.getIntValue("machineId"),
        duration_seconds = obj.getDoubleValue("duration_seconds"),
        is_running = obj.getIntValue("is_running"),
        is_standby = obj.getIntValue("is_standby"),
        is_offline = obj.getIntValue("is_offline"),
        isAlarm = Option(obj.getString("isAlarm")).getOrElse(""),
        cumulative_alarms = obj.getIntValue("cumulative_alarms")
      ))
    } catch {
      case e: Exception =>
        println(s"[警告] device_state JSON 解析失败: ${e.getMessage}")
        None
    }
  }

  private def parseSensorMetrics(jsonStr: String): Option[SensorMetrics] = {
    try {
      val obj = JSON.parseObject(jsonStr)
      Some(SensorMetrics(
        machineId = obj.getIntValue("machineId"),
        cutting_time = obj.getDoubleValue("cutting_time"),
        cycle_time = obj.getDoubleValue("cycle_time"),
        spindle_load = obj.getDoubleValue("spindle_load"),
        feed_rate = obj.getIntValue("feed_rate"),
        spindle_speed = obj.getIntValue("spindle_speed"),
        total_parts = obj.getIntValue("total_parts"),
        cumulative_runtime = obj.getDoubleValue("cumulative_runtime"),
        cumulative_parts = obj.getIntValue("cumulative_parts"),
        avg_spindle_load_10 = obj.getDoubleValue("avg_spindle_load_10"),
        avg_cutting_time_10 = obj.getDoubleValue("avg_cutting_time_10")
      ))
    } catch {
      case e: Exception =>
        println(s"[警告] sensor_metrics JSON 解析失败: ${e.getMessage}")
        None
    }
  }

  private def parseHighFreqSensor(jsonStr: String): Option[HighFreqSensor] = {
    try {
      val obj = JSON.parseObject(jsonStr)
      Some(HighFreqSensor(
        machineId = obj.getIntValue("machineId"),
        timestamp = obj.getString("timestamp"),
        temperature = obj.getDoubleValue("temperature"),
        vibration_x = obj.getDoubleValue("vibration_x"),
        vibration_y = obj.getDoubleValue("vibration_y"),
        vibration_z = obj.getDoubleValue("vibration_z"),
        spindle_load = obj.getDoubleValue("spindle_load"),
        feed_rate = obj.getIntValue("feed_rate"),
        spindle_speed = obj.getIntValue("spindle_speed"),
        is_running = obj.getIntValue("is_running")
      ))
    } catch {
      case e: Exception =>
        println(s"[警告] highfreq_sensor JSON 解析失败: ${e.getMessage}")
        None
    }
  }

  // ==================== 核心处理：低频双流关联 + 报警转译 ====================

  class LowFreqCoProcessFunction
      extends CoProcessFunction[DeviceState, SensorMetrics, AlarmRecord] {

    private val stateBuffer: mutable.Map[Int, DeviceState] = mutable.Map.empty
    private val metricsBuffer: mutable.Map[Int, SensorMetrics] = mutable.Map.empty

    override def processElement1(
        state: DeviceState,
        ctx: CoProcessFunction[DeviceState, SensorMetrics, AlarmRecord]#Context,
        out: Collector[AlarmRecord]
    ): Unit = {
      metricsBuffer.get(state.machineId) match {
        case Some(m) =>
          emitIfAlarm(state, m, out)
          metricsBuffer.remove(state.machineId)
        case None =>
          stateBuffer(state.machineId) = state
      }
    }

    override def processElement2(
        metrics: SensorMetrics,
        ctx: CoProcessFunction[DeviceState, SensorMetrics, AlarmRecord]#Context,
        out: Collector[AlarmRecord]
    ): Unit = {
      stateBuffer.get(metrics.machineId) match {
        case Some(s) =>
          emitIfAlarm(s, metrics, out)
          stateBuffer.remove(metrics.machineId)
        case None =>
          metricsBuffer(metrics.machineId) = metrics
      }
    }

    private def emitIfAlarm(
        state: DeviceState,
        metrics: SensorMetrics,
        out: Collector[AlarmRecord]
    ): Unit = {
      if (state.isAlarm != null && state.isAlarm.nonEmpty) {
        val alarmMsg = ALARM_DICTIONARY.getOrElse(
          state.isAlarm, s"未知错误码: ${state.isAlarm}"
        )

        val record = AlarmRecord(
          machineId = state.machineId,
          duration_seconds = state.duration_seconds,
          is_running = state.is_running,
          is_standby = state.is_standby,
          is_offline = state.is_offline,
          cutting_time = metrics.cutting_time,
          cycle_time = metrics.cycle_time,
          total_parts = metrics.total_parts,
          spindle_load = metrics.spindle_load,
          cumulative_runtime = metrics.cumulative_runtime,
          cumulative_parts = metrics.cumulative_parts,
          cumulative_alarms = state.cumulative_alarms,
          avg_spindle_load_10 = metrics.avg_spindle_load_10,
          avg_cutting_time_10 = metrics.avg_cutting_time_10,
          // 高频窗口特征 (当前版本占位，三流合并后填入真实值)
          var_temp = 0.0,
          kurtosis_temp = 0.0,
          fft_peak = 0.0,
          avg_spindle_load_win = 0.0,
          max_spindle_load_win = 0.0,
          running_ratio = 0.0,
          avg_feed_rate = 0.0,
          alarmMessage = alarmMsg
        )

        out.collect(record)
        println(s"[报警] 设备${record.machineId} => ${record.alarmMessage}")

      }
      // 无报警时不输出
    }
  }

}
