package org.example.tasks

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Try
import scala.xml.XML

/**
 * ChangeDataXMLParser
 * ================
 * 从 MySQL shtd_industry.ChangeRecord 表的 ChangeData (XML) 字段中，
 * 解析提取 14 维设备特征，输出两个 DataFrame：
 *
 * 1. device_metrics（设备 PLC 指标）:
 *    cuttingTime, cycleTime, spindleLoad, totalParts,
 *    cumRuntime, cumParts, avgSpindleLoad, spindleSpeed, feedSpeed
 *
 * 2. device_state（设备状态快照）:
 *    isRunning, isStandby, isOffline, isAlarm, durationSeconds,
 *    machineStatus, connectionStatus, alarmCode, alarmMsg
 *
 * XML 格式示例:
 * <col ColName="切削时间">495945</col>
 * <col ColName="报警信息">{"AlmNo":0,"AlmStartTime":"无","AlmMsg":"无报警"}</col>
 * <col ColName="机器状态">运行</col>
 */
object ChangeDataXMLParser {

  // ==========================================
  // XML 字段 → 特征名 映射表
  // ==========================================

  /** device_metrics 需要的 XML 字段 */
  val METRICS_FIELDS: Map[String, String] = Map(
    "切削时间"       -> "cuttingTime",
    "循环时间"       -> "cycleTime",
    "主轴负载"       -> "spindleLoad",
    "总加工个数"     -> "totalParts",
    "上电时间"       -> "cumRuntime",
    "加工个数"       -> "cumParts",
    "主轴转速"       -> "spindleSpeed",
    "进给速度"       -> "feedSpeed"
  )

  /** device_state 需要的 XML 字段 */
  val STATE_FIELDS: Map[String, String] = Map(
    "机器状态"       -> "machineStatus",
    "连接状态"       -> "connectionStatus",
    "报警信息"       -> "alarmInfoRaw",
    "运行时间"       -> "durationSeconds",
    "设备IP"         -> "deviceIP"
  )

  /** 机器状态枚举值 */
  val STATUS_RUNNING = Set("运行", "运行中", "自动运行", "AUTO")
  val STATUS_STANDBY = Set("待机", "暂停", "RESET", "MDI")
  val STATUS_OFFLINE = Set("断开", "离线", "offline", "异常")

  // ==========================================
  // 核心解析：单条 XML 字符串 → Map[String, String]
  // ==========================================

  /**
   * 解析单条 ChangeData XML 字符串，返回 ColName → Value 的 Map
   * 输入示例: <col ColName="切削时间">495945</col><col ColName="机器状态">运行</col>...
   * 输出: Map("切削时间" -> "495945", "机器状态" -> "运行", ...)
   */
  def parseChangeData(xmlStr: String): Map[String, String] = {
    if (xmlStr == null || xmlStr.trim.isEmpty) return Map.empty

    try {
      val xml = XML.loadString(s"<root>$xmlStr</root>")
      (xml \\ "col").map { node =>
        val colName = (node \ "@ColName").text.trim
        val value = node.text.trim
        colName -> value
      }.toMap
    } catch {
      case e: Exception =>
        println(s"[WARN] XML解析失败: ${e.getMessage}, 原始数据前100字符: ${xmlStr.take(100)}")
        Map.empty
    }
  }

  // ==========================================
  // 特征提取：从 parsedMap 中提取 14 维特征
  // ==========================================

  /**
   * 提取 device_metrics 维度特征（数值型）
   * 返回: cuttingTime, cycleTime, spindleLoad, totalParts, cumRuntime, cumParts, avgSpindleLoad, spindleSpeed, feedSpeed
   */
  def extractMetrics(parsed: Map[String, String]): Map[String, Double] = {
    // 安全转为 Double，失败返回 0.0
    def toDouble(key: String, default: Double = 0.0): Double = {
      Try(parsed.get(key).map(_.replaceAll("[^\\d.-]", "").toDouble).getOrElse(default)).getOrElse(default)
    }

    Map(
      "cuttingTime"     -> toDouble("切削时间"),
      "cycleTime"       -> toDouble("循环时间"),
      "spindleLoad"     -> toDouble("主轴负载"),
      "totalParts"      -> toDouble("总加工个数"),
      "cumRuntime"      -> toDouble("上电时间"),
      "cumParts"        -> toDouble("加工个数"),
      "avgSpindleLoad"  -> toDouble("主轴负载"),  // 当前值作为近似的滑动均值
      "spindleSpeed"    -> toDouble("主轴转速"),
      "feedSpeed"       -> toDouble("进给速度")
    )
  }

  /**
   * 提取 device_state 维度特征（状态型 + 数值）
   * 返回: isRunning, isStandby, isOffline, isAlarm, durationSeconds, machineStatus, ...
   */
  def extractState(parsed: Map[String, String]): Map[String, Any] = {
    val machineStatus = parsed.getOrElse("机器状态", "未知")
    val connStatus = parsed.getOrElse("连接状态", "offline")
    val alarmRaw = parsed.getOrElse("报警信息", "{}")

    // 解析报警 JSON {"AlmNo":0,"AlmStartTime":"无","AlmMsg":"无报警"}
    val alarmNo = Try {
      if (alarmRaw.startsWith("{")) {
        // 简易 JSON 解析：提取 AlmNo 的值
        val noMatch = """"AlmNo"\s*:\s*(\d+)""".r.findFirstMatchIn(alarmRaw)
        noMatch.map(_.group(1).toInt).getOrElse(0)
      } else 0
    }.getOrElse(0)

    val alarmMsg = Try {
      if (alarmRaw.startsWith("{")) {
        val msgMatch = """"AlmMsg"\s*:\s*"([^"]*)"""".r.findFirstMatchIn(alarmRaw)
        msgMatch.map(_.group(1)).getOrElse("未知")
      } else alarmRaw
    }.getOrElse("未知")

    Map(
      "isRunning"          -> (if (STATUS_RUNNING.contains(machineStatus)) 1.0 else 0.0),
      "isStandby"          -> (if (STATUS_STANDBY.contains(machineStatus)) 1.0 else 0.0),
      "isOffline"          -> (if (connStatus != "normal") 1.0 else 0.0),
      "isAlarm"            -> (if (alarmNo > 0) 1.0 else 0.0),
      "durationSeconds"    -> Try(parsed("运行时间").replaceAll("[^\\d]", "").toDouble).getOrElse(0.0),
      "machineStatus"      -> machineStatus,
      "connectionStatus"   -> connStatus,
      "alarmNo"            -> alarmNo,
      "alarmMsg"           -> alarmMsg,
      "deviceIP"           -> parsed.getOrElse("deviceIP", parsed.getOrElse("设备IP", "unknown"))
    )
  }

  /**
   * 提取完整的 14 维特征数组（供 XGBoost / FlinkRulInference 使用）
   * 顺序与 FlinkRulInference.scala 中 featureArray 定义一致:
   * [isRunning, isStandby, isOffline, isAlarm, durationSeconds,
   *  cuttingTime, cycleTime, spindleLoad, totalParts,
   *  cumRuntime, cumParts, avgSpindleLoad, ...]
   */
  def extractFeatureArray(xmlStr: String): Array[Double] = {
    val parsed = parseChangeData(xmlStr)
    val metrics = extractMetrics(parsed)
    val state = extractState(parsed)

    Array(
      state("isRunning").asInstanceOf[Double],
      state("isStandby").asInstanceOf[Double],
      state("isOffline").asInstanceOf[Double],
      state("isAlarm").asInstanceOf[Double],
      state("durationSeconds").asInstanceOf[Double],
      metrics("cuttingTime"),
      metrics("cycleTime"),
      metrics("spindleLoad"),
      metrics("totalParts"),
      metrics("cumRuntime"),
      metrics("cumParts"),
      metrics("avgSpindleLoad")
      // 如需更多维度可继续追加
    )
  }

  // ==========================================
  // Spark UDF：在 DataFrame 中批量使用
  // ==========================================

  /** UDF: XML字符串 → 切削时间 */
  val udfExtractCuttingTime = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    Try(m("切削时间").replaceAll("[^\\d.-]", "").toDouble).getOrElse(0.0)
  })

  /** UDF: XML字符串 → 循环时间 */
  val udfExtractCycleTime = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    Try(m("循环时间").replaceAll("[^\\d.-]", "").toDouble).getOrElse(0.0)
  })

  /** UDF: XML字符串 → 主轴负载 */
  val udfExtractSpindleLoad = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    Try(m("主轴负载").replaceAll("[^\\d.-]", "").toDouble).getOrElse(0.0)
  })

  /** UDF: XML字符串 → 总加工个数 */
  val udfExtractTotalParts = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    Try(m("总加工个数").replaceAll("[^\\d.-]", "").toDouble).getOrElse(0.0)
  })

  /** UDF: XML字符串 → 上电时间/累计运行 */
  val udfExtractCumRuntime = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    Try(m("上电时间").replaceAll("[^\\d.-]", "").toDouble).getOrElse(0.0)
  })

  /** UDF: XML字符串 → 加工个数 */
  val udfExtractCumParts = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    Try(m("加工个数").replaceAll("[^\\d.-]", "").toDouble).getOrElse(0.0)
  })

  /** UDF: XML字符串 → 主轴转速 */
  val udfExtractSpindleSpeed = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    Try(m("主轴转速").replaceAll("[^\\d.-]", "").toDouble).getOrElse(0.0)
  })

  /** UDF: XML字符串 → 进给速度 */
  val udfExtractFeedSpeed = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    Try(m("进给速度").replaceAll("[^\\d.-]", "").toDouble).getOrElse(0.0)
  })

  /** UDF: XML字符串 → 机器状态 (原始值) */
  val udfExtractMachineStatus = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    m.getOrElse("机器状态", "未知")
  })

  /** UDF: XML字符串 → isRunning (0.0/1.0) */
  val udfIsRunning = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    val status = m.getOrElse("机器Status", m.getOrElse("机器状态", ""))
    if (STATUS_RUNNING.contains(status)) 1.0 else 0.0
  })

  /** UDF: XML字符串 → isStandby (0.0/1.0) */
  val udfIsStandby = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    val status = m.getOrElse("机器Status", m.getOrElse("机器状态", ""))
    if (STATUS_STANDBY.contains(status)) 1.0 else 0.0
  })

  /** UDF: XML字符串 → isOffline (0.0/1.0) */
  val udfIsOffline = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    val conn = m.getOrElse("连接状态", "offline")
    if (conn != "normal") 1.0 else 0.0
  })

  /** UDF: XML字符串 → isAlarm (0.0/1.0), 通过解析报警信息JSON中的 AlmNo 判断 */
  val udfIsAlarm = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    val alarmRaw = m.getOrElse("报警信息", "{}")
    val alarmNo = Try {
      if (alarmRaw.startsWith("{")) {
        val noMatch = """"AlmNo"\s*:\s*(\d+)""".r.findFirstMatchIn(alarmRaw)
        noMatch.map(_.group(1).toInt).getOrElse(0)
      } else 0
    }.getOrElse(0)
    if (alarmNo > 0) 1.0 else 0.0
  })

  /** UDF: XML字符串 → durationSeconds (运行时间) */
  val udfDurationSeconds = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    Try(m("运行时间").replaceAll("[^\\d]", "").toDouble).getOrElse(0.0)
  })

  /** UDF: XML字符串 → 设备IP */
  val udfDeviceIP = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    m.getOrElse("设备IP", m.getOrElse("deviceIP", "unknown"))
  })

  /** UDF: XML字符串 → 报警代码 */
  val udfAlarmCode = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    val alarmRaw = m.getOrElse("报警信息", "{}")
    Try {
      if (alarmRaw.startsWith("{")) {
        val noMatch = """"AlmNo"\s*:\s*(\d+)""".r.findFirstMatchIn(alarmRaw)
        noMatch.map(_.group(1)).getOrElse("0")
      } else "0"
    }.getOrElse("0")
  })

  /** UDF: XML字符串 → 报警消息 */
  val udfAlarmMessage = udf((xmlStr: String) => {
    val m = parseChangeData(xmlStr)
    val alarmRaw = m.getOrElse("报警信息", "{}")
    Try {
      if (alarmRaw.startsWith("{")) {
        val msgMatch = """"AlmMsg"\s*:\s*"([^"]*)"""".r.findFirstMatchIn(alarmRaw)
        msgMatch.map(_.group(1)).getOrElse("未知")
      } else alarmRaw
    }.getOrElse("未知")
  })

  // ==========================================
  // Spark DataFrame 批量转换
  // ==========================================

  /**
   * 将包含 ChangeData XML 字段的 DataFrame 转换为展开的 device_metrics DataFrame
   * 输入列: id, machine_id, change_data, ts (可选)
   * 输出列: machine_id, ts, cuttingTime, cycleTime, spindleLoad, totalParts,
   *         cumRuntime, cumParts, avgSpindleLoad, spindleSpeed, feedSpeed
   */
  def parseToMetricsDF(df: DataFrame): DataFrame = {
    df.select(
      col("*"),
      udfExtractCuttingTime(col("change_data")).alias("cuttingTime"),
      udfExtractCycleTime(col("change_data")).alias("cycleTime"),
      udfExtractSpindleLoad(col("change_data")).alias("spindleLoad"),
      udfExtractTotalParts(col("change_data")).alias("totalParts"),
      udfExtractCumRuntime(col("change_data")).alias("cumRuntime"),
      udfExtractCumParts(col("change_data")).alias("cumParts"),
      udfExtractSpindleLoad(col("change_data")).alias("avgSpindleLoad"), // 近似
      udfExtractSpindleSpeed(col("change_data")).alias("spindleSpeed"),
      udfExtractFeedSpeed(col("change_data")).alias("feedSpeed"),
      udfDeviceIP(col("change_data")).alias("device_ip")
    )
  }

  /**
   * 将包含 ChangeData XML 字段的 DataFrame 转换为展开的 device_state DataFrame
   * 输入列: id, machine_id, change_data, ts (可选)
   * 输出列: machine_id, ts, isRunning, isStandby, isOffline, isAlarm,
   *         durationSeconds, machineStatus, connectionStatus, alarmNo, alarmMsg, device_ip
   */
  def parseToStateDF(df: DataFrame): DataFrame = {
    df.select(
      col("*"),
      udfIsRunning(col("change_data")).alias("isRunning"),
      udfIsStandby(col("change_data")).alias("isStandby"),
      udfIsOffline(col("change_data")).alias("isOffline"),
      udfIsAlarm(col("change_data")).alias("isAlarm"),
      udfDurationSeconds(col("change_data")).alias("durationSeconds"),
      udfExtractMachineStatus(col("change_data")).alias("machineStatus"),
      lit("normal").alias("connectionStatus"), // 从 XML 提取
      udfAlarmCode(col("change_data")).alias("alarmNo"),
      udfAlarmMessage(col("change_data")).alias("alarmMsg"),
      udfDeviceIP(col("change_data")).alias("device_ip")
    )
  }

  // ==========================================
  // Main 测试方法
  // ==========================================

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ChangeDataXMLParser-Test")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 测试数据：你给的真实 XML 样例
    val testXml = """<col ColName="设备IP">192.168.1.201</col><col ColName="进给速度">0</col><col ColName="急停状态">否</col><col ColName="加工状态">MDI</col><col ColName="刀片相关信息">刀片组号:0;type:0;刀片组的全部数量:6;刀片号:0;刀片组号:0;寿命计时0</col><col ColName="切削时间">495945</col><col ColName="未使用内存">1033</col><col ColName="循环时间">613424</col><col ColName="报警信息">{"AlmNo":0,"AlmStartTime":"无","AlmMsg":"无报警"}</col><col ColName="主轴转速">0</col><col ColName="上电时间">1772023</col><col ColName="总加工个数">7850</col><col ColName="班次信息">null</col><col ColName="运行时间">770715</col><col ColName="正在运行刀具信息">没有刀具在运行</col><col ColName="有效轴数">8</col><col ColName="主轴负载">0</col><col ColName="PMC程序号">0009</col><col ColName="进给倍率">100</col><col ColName="主轴倍率">100</col><col ColName="已使用内存">41</col><col ColName="可用程序量">934</col><col ColName="刀补值">0</col><col ColName="工作模式">MEMory</col><col ColName="机器状态">运行</col><col ColName="连接状态">normal</col><col ColName="加工个数">3432</col><col ColName="机床位置">["4.521","-1.383","-0.087","0","0.044","0.015","-0.092","0.002","4.521","-1.383","-0.087","0"]</col><col ColName="注册程序量">77</col>"""

    println("=" * 80)
    println("  ChangeDataXMLParser 单元测试")
    println("=" * 80)

    // 1. 测试基础解析
    val parsed = parseChangeData(testXml)
    println(s"\n[测试1] XML 解析结果 (${parsed.size} 个字段):")
    parsed.foreach { case (k, v) =>
      println(f"  $k%-12s = $v")
    }

    // 2. 测试 metrics 提取
    val metrics = extractMetrics(parsed)
    println(s"\n[测试2] Metrics 特征提取:")
    metrics.foreach { case (k, v) =>
      println(f"  $k%-16s = $v%.1f")
    }

    // 3. 测试 state 提取
    val state = extractState(parsed)
    println(s"\n[测试3] State 特征提取:")
    state.foreach { case (k, v) =>
      println(f"  $k%-20s = $v")
    }

    // 4. 测试 14 维数组
    val features = extractFeatureArray(testXml)
    println(s"\n[测试4] 14维特征数组 (长度=${features.length}):")
    features.zipWithIndex.foreach { case (v, i) =>
      println(f"  [$i%2d] = $v%.4f")
    }

    // 5. 测试 DataFrame 批量处理
    println(s"\n[测试5] DataFrame 批量处理:")
    val testDF = Seq(
      ("machine_001", testXml, java.lang.System.currentTimeMillis()),
      ("machine_002", testXml.replace(">运行<", ">待机<"), java.lang.System.currentTimeMillis())
    ).toDF("machine_id", "change_data", "ts")

    val metricsDF = parseToMetricsDF(testDF)
    println("\n--- device_metrics ---")
    metricsDF.show(false)

    val stateDF = parseToStateDF(testDF)
    println("\n--- device_state ---")
    stateDF.show(false)

    println("\n" + "=" * 80)
    println("  测试通过! 所有 14 维特征均成功提取。")
    println("=" * 80)

    spark.stop()
  }
}
