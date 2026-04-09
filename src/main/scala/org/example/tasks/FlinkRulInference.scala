package org.example.tasks

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, CoProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}

import java.time.Duration
import java.util.Properties
import com.alibaba.fastjson.JSON

import scala.collection.mutable
import org.apache.kafka.clients.producer.ProducerRecord

import java.lang

object FlinkRulInference {

  val ALARM_DICTIONARY: Map[String, String] = Map(
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

    "0x000E1" -> "产线急停被按下，需顺时针释放",
    "0x000E2" -> "安全门打开，联锁未闭合",
    "0x000E3" -> "PLC与上位机通讯故障",
    "0x000E4" -> "交换机异常，网络中断",
    "0x000E5" -> "产线总电源异常，电压波动"
  )

  import org.example.tasks.utils.FeatureUtils

  case class DeviceState(
      machineId: Int,
      duration_seconds: Double,
      is_running: Int,
      is_standby: Int,
      is_offline: Int,
      isAlarm: String,
      cumulative_alarms: Int
  )

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
    def this() =
      this(0L, 0.0, 0.0, 0.0, 0.0, 0.0, Double.MinValue, 0.0, 0L, 0.0, 0.0)

  }

  case class TimeSeriesFeatures(
      machineId: Int,
      var_temp: Double, // 温度方差
      kurtosis_temp: Double, // 温度峭度 (>3=尖峰, <3=平坦)
      fft_peak: Double, // 振动频域峰值能量
      avg_spindle_load: Double, // 窗口平均主轴负载
      max_spindle_load: Double, // 窗口峰值主轴负载
      running_ratio: Double, // 运行占比 [0~1]
      avg_feed_rate: Double // 窗口平均进给速度
  )

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

  class HighFreqAggregator
      extends AggregateFunction[
        HighFreqSensor,
        HighFreqAccumulator,
        TimeSeriesFeatures
      ] {

    override def createAccumulator(): HighFreqAccumulator =
      new HighFreqAccumulator()

    override def add(
        value: HighFreqSensor,
        acc: HighFreqAccumulator
    ): HighFreqAccumulator = FeatureUtils.accumulateFeatures(value, acc)

    override def merge(
        a: HighFreqAccumulator,
        b: HighFreqAccumulator
    ): HighFreqAccumulator = FeatureUtils.mergeAccumulators(a, b)

    override def getResult(acc: HighFreqAccumulator): TimeSeriesFeatures =
      FeatureUtils.calculateTimeSeriesFeatures(acc)
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaProps = new Properties()
    kafkaProps.setProperty(
      "bootstrap.servers",
      AppConfig.getString("kafka.bootstrap.servers", "master:9092")
    )
    kafkaProps.setProperty("group.id", "flink-feature-engine-group")

    println("=" * 70)
    println("  Flink 实时特征计算引擎")
    println("  Feature Engine + Alarm Translator v2.0")
    println("=" * 70)

    println(s">>> [1/3] 订阅 Kafka topic: device_state")
    val stateConsumer = new FlinkKafkaConsumer[String](
      "device_state",
      new SimpleStringSchema(),
      kafkaProps
    )
    stateConsumer.setStartFromLatest()

    val stateStream = env
      .addSource(stateConsumer)
      .name("Kafka-DeviceState")
      .map(parseDeviceState _)
      .name("Parse-DeviceState")
      .filter(_.isDefined)
      .map(_.get)

    println(s">>> [1/3] 订阅 Kafka topic: device_state")

    val metricsConsumer = new FlinkKafkaConsumer[String](
      "sensor_metrics",
      new SimpleStringSchema(),
      kafkaProps
    )
    metricsConsumer.setStartFromLatest()

    val metricsStream = env
      .addSource(metricsConsumer)
      .name("Kafka-SensorMetrics")
      .map(parseSensorMetrics _)
      .name("Parse-SensorMetrics")
      .filter(_.isDefined)
      .map(_.get)

    println(s">>> [2/3] 订阅 Kafka topic: sensor_metrics")

    val highFreqConsumer = new FlinkKafkaConsumer[String](
      "highfreq_sensor",
      new SimpleStringSchema(),
      kafkaProps
    )
    highFreqConsumer.setStartFromLatest()

    val highFreqStream = env
      .addSource(highFreqConsumer)
      .name("Kafka-HighFreqSensor")
      .map { jsonStr =>
        parseHighFreqSensor(jsonStr)
      }
      .name("Parse-HighFreqSensor")
      .filter(_.isDefined)
      .map(_.get)

    println(s">>> [3/3] 订阅 Kafka topic: highfreq_sensor (高频)")

    // ========== 核心步骤1: 高频数据使用1分钟tumbling window聚合计算7个时序特征 ==========
    val windowSize=Time.minutes(1)
    val timeSeriesFeatures:DataStream[TimeSeriesFeatures]=highFreqStream
      .keyBy(_.machineId)
      .window(TumblingProcessingTimeWindows.of(windowSize))
      .aggregate(
        new HighFreqAggregator,
        new ProcessWindowFunction[TimeSeriesFeatures,TimeSeriesFeatures,Int,TimeWindow] {
          override def process(key: Int, context: Context, elements: Iterable[TimeSeriesFeatures], out: Collector[TimeSeriesFeatures]): Unit = {
            elements.foreach{f=>val result=f.copy(machineId=key)
              out.collect(result)
            }
          }
        }
      )
      .name("HighFreq-Features")

    val highFreqStateDescriptor=new MapStateDescriptor[Int,TimeSeriesFeatures](
      "highfreq",
      classOf[Int],
      classOf[TimeSeriesFeatures]
    )

    val highFreqBroadcast:BroadcastStream[TimeSeriesFeatures]=timeSeriesFeatures
      .broadcast(highFreqStateDescriptor)

    val lowFreqConnected=stateStream
      .connect(metricsStream)
      .keyBy(_.machineId,_.machineId)
      .process(new LowFreqCoProcessFunction)
      .name("Merge-HighFreq")

    // ========== 核心步骤2: 使用Broadcast State对齐高频和低频数据 ==========
    // 连接低频数据流和高频广播流，实现数据对齐
    val mergedStream=lowFreqConnected
      .keyBy(_.machineId)
      .connect(highFreqBroadcast)
      .process(new MergeHighFreqFunction(highFreqStateDescriptor))
      .name("Merge-HighFreq")


    val prop=new Properties()
    prop.setProperty("bootstrap.servers",kafkaProps.getProperty("bootstrap.servers"))

    val warningSink=new FlinkKafkaProducer[String](
      "feature_log",
      new KafkaSerializationSchema[String] {
        override def serialize(t: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
          new ProducerRecord("Feature_log", t.getBytes("UTF-8"))
        }
      },
      prop,
      FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
    )

    // ========== 输出22维特征向量到Feature_log供LLM使用 ==========
    // 将合并后的22维特征向量序列化为JSON格式
    mergedStream
      .map { r: AlarmRecord => // 遍历每条AlarmRecord记录
        val hasAlarm = r.alarmMessage != null && r.alarmMessage.nonEmpty // 判断是否有报警信息
        val alarmMsg = if (hasAlarm) r.alarmMessage else "" // 提取报警消息，无报警则为空字符串
        // 手动构建JSON字符串（避免Scala case class与fastjson序列化兼容性问题）
        s"""{"machineId":${r.machineId},""" + // 设备ID
          s""""duration_seconds":${r.duration_seconds},""" + // 持续时间（秒）
          s""""is_running":${r.is_running},""" + // 是否运行中
          s""""is_standby":${r.is_standby},""" + // 是否待机
          s""""is_offline":${r.is_offline},""" + // 是否离线
          s""""cutting_time":${r.cutting_time},""" + // 切削时间
          s""""cycle_time":${r.cycle_time},""" + // 循环周期时间
          s""""total_parts":${r.total_parts},""" + // 总加工零件数
          s""""spindle_load":${r.spindle_load},""" + // 主轴负载
          s""""cumulative_runtime":${r.cumulative_runtime},""" + // 累计运行时间
          s""""cumulative_parts":${r.cumulative_parts},""" + // 累计加工零件数
          s""""cumulative_alarms":${r.cumulative_alarms},""" + // 累计报警次数
          s""""avg_spindle_load_10":${r.avg_spindle_load_10},""" + // 近10次平均主轴负载
          s""""avg_cutting_time_10":${r.avg_cutting_time_10},""" + // 近10次平均切削时间
          s""""var_temp":${r.var_temp},""" + // 温度方差（高频特征1）
          s""""kurtosis_temp":${r.kurtosis_temp},""" + // 温度峭度（高频特征2）
          s""""fft_peak":${r.fft_peak},""" + // 振动频域峰值（高频特征3）
          s""""avg_spindle_load_win":${r.avg_spindle_load_win},""" + // 窗口平均主轴负载（高频特征4）
          s""""max_spindle_load_win":${r.max_spindle_load_win},""" + // 窗口峰值主轴负载（高频特征5）
          s""""running_ratio":${r.running_ratio},""" + // 运行占比（高频特征6）
          s""""avg_feed_rate":${r.avg_feed_rate},""" + // 窗口平均进给速度（高频特征7）
          s""""alarmMessage":"${escapeJson(alarmMsg)}"}""" // 报警中文描述（转义特殊字符）
      }
      .addSink(warningSink) // 写入Kafka的Feature_log topic
      .name("Kafka-Sink-AllFeatures")

    println("-" * 70)
    println(s"  Kafka brokers : ${kafkaProps.getProperty("bootstrap.servers")}")
    println("  输入 topics   :")
    println("    ├─ device_state     (低频, 状态日志)")
    println("    ├─ sensor_metrics   (低频, 传感器)")
    println("    └─ highfreq_sensor  (高频, 时序采样)")
    println("")
    println("  窗口聚合 (highfreq_sensor):")
    println(s"    └─ TumblingWindow = 1 分钟")
    println("       输出 7 个时序特征:")
    println("         var_temp, kurtosis_temp, fft_peak,")
    println("         avg/max_spindle_load, running_ratio, avg_feed_rate")
    println("")
    println("  输出 topics:")
    println("    └─ Feature_log (22维特征向量: 14低频+7高频+alarmMessage)")
    println(f"  报警字典条目数: ${ALARM_DICTIONARY.size}%d 条")
    println("-" * 70)
    println("  等待数据流... (Ctrl+C 停止)")
    println("=" * 70)

    env.execute("Flink-FeatureEngine-v2")
  }

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
      Some(
        DeviceState(
          machineId = obj.getIntValue("machineId"),
          duration_seconds = obj.getDoubleValue("duration_seconds"),
          is_running = obj.getIntValue("is_running"),
          is_standby = obj.getIntValue("is_standby"),
          is_offline = obj.getIntValue("is_offline"),
          isAlarm = Option(obj.getString("isAlarm")).getOrElse(""),
          cumulative_alarms = obj.getIntValue("cumulative_alarms")
        )
      )
    } catch {
      case e: Exception =>
        println(s"[警告] device_state JSON 解析失败: ${e.getMessage}")
        None
    }
  }

  private def parseSensorMetrics(jsonStr: String): Option[SensorMetrics] = {
    try {
      val obj = JSON.parseObject(jsonStr)
      Some(
        SensorMetrics(
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
        )
      )
    } catch {
      case e: Exception =>
        println(s"[警告] sensor_metrics JSON 解析失败: ${e.getMessage}")
        None
    }
  }

  private def parseHighFreqSensor(jsonStr: String): Option[HighFreqSensor] = {
    try {
      val obj = JSON.parseObject(jsonStr)
      Some(
        HighFreqSensor(
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
        )
      )
    } catch {
      case e: Exception =>
        println(s"[警告] highfreq_sensor JSON 解析失败: ${e.getMessage}")
        None
    }
  }

  class LowFreqCoProcessFunction
      extends CoProcessFunction[DeviceState, SensorMetrics, AlarmRecord] {

    private val stateBuffer: mutable.Map[Int, DeviceState] = mutable.Map.empty
    private val metricsBuffer: mutable.Map[Int, SensorMetrics] =
      mutable.Map.empty

    override def processElement1(
        state: DeviceState,
        ctx: CoProcessFunction[DeviceState, SensorMetrics, AlarmRecord]#Context,
        out: Collector[AlarmRecord]
    ): Unit = {
      metricsBuffer.get(state.machineId) match {
        case Some(m) =>
          emitRecord(state, m, out)
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
          emitRecord(s, metrics, out)
          stateBuffer.remove(metrics.machineId)
        case None =>
          metricsBuffer(metrics.machineId) = metrics
      }
    }

    private def emitRecord(
        state: DeviceState,
        metrics: SensorMetrics,
        out: Collector[AlarmRecord]
    ): Unit = {
      val hasAlarm = state.isAlarm != null && state.isAlarm.nonEmpty
      val alarmMsg = if (hasAlarm) {
        ALARM_DICTIONARY.getOrElse(state.isAlarm, s"未知错误码: ${state.isAlarm}")
      } else ""

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
    }
  }

  class MergeHighFreqFunction(highFreqStateDescriptor: MapStateDescriptor[Int, TimeSeriesFeatures]) extends KeyedBroadcastProcessFunction[Int, AlarmRecord, TimeSeriesFeatures, AlarmRecord] {
    override def processElement(record: AlarmRecord, ctx: KeyedBroadcastProcessFunction[Int, AlarmRecord, TimeSeriesFeatures, AlarmRecord]#ReadOnlyContext, out: Collector[AlarmRecord]): Unit = {
      // 从 Broadcast State 读取当前设备的高频窗口特征
      val highFreqState: ReadOnlyBroadcastState[Int, TimeSeriesFeatures] = ctx.getBroadcastState(highFreqStateDescriptor)
      val features = highFreqState.get(record.machineId) // 根据machineId查询高频特征

      val mergedRecord = if (features != null) {
        // 如果存在高频特征，将其填充到AlarmRecord中
        FeatureUtils.fillAlarmRecordWithFeatures(record, features) // 合并14个低频特征 + 7个高频特征 = 21个数值特征
      } else {
        // 如果还没有高频特征数据，保持默认值0.0（等待下一个窗口）
        record
      }
      out.collect(mergedRecord) // 输出合并后的完整记录
    }

    override def processBroadcastElement(features: TimeSeriesFeatures, ctx: KeyedBroadcastProcessFunction[Int, AlarmRecord, TimeSeriesFeatures, AlarmRecord]#Context, out: Collector[AlarmRecord]): Unit = {
      // 接收广播的高频窗口特征并更新Broadcast State
      val highFreqState: BroadcastState[Int, TimeSeriesFeatures] = ctx.getBroadcastState(highFreqStateDescriptor)
      highFreqState.put(features.machineId, features) // 将新的高频特征存入Broadcast State，key为设备ID
    }
  }
}
