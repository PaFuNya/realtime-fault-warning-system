package org.example.tasks

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Properties
import java.text.SimpleDateFormat
import java.util.Date

/**
 * ChangeDataCleaner
 * ================
 * 专一职责：从 MySQL shtd_industry.ChangeRecord 表读取 ChangeData (XML) 字段，
 * 解析提取 14 维设备特征，输出为结构化的 CSV / JSON 文件。
 *
 * ======== 数据流 ========
 * MySQL ChangeRecord 表 (881行)
 *   ↓ JDBC 读取
 * ChangeDataXMLParser 解析 XML → 提取14维特征
 *   ↓
 * ┌────────────────────┬──────────────────────┐
 │ device_metrics.csv  │ device_state.csv     │
 │ (9个PLC数值指标)    │ (6个状态+5个扩展字段)  │
 └────────────────────┴──────────────────────┘
 *
 * ======== 输出的 14 个特征 ========
 *
 * 【device_metrics - PLC 数值指标】
 *   1. cuttingTime     切削时间(秒)
 *   2. cycleTime       循环时间(秒)
 *   3. spindleLoad     主轴负载(%)
 *   4. totalParts      总加工个数
 *   5. cumRuntime      上电累计运行(秒)
 *   6. cumParts        本次加工个数
 *   7. avgSpindleLoad  主轴负载均值(%)
 *   8. spindleSpeed    主轴转速(RPM)
 *   9. feedSpeed       进给速度(mm/min)
 *
 * 【device_state - 设备状态】
 *  10. isRunning       是否运行中 (0/1)
 *  11. isStandby       是否待机 (0/1)
 *  12. isOffline       是否离线 (0/1)
 *  13. isAlarm         是否报警 (0/1)
 *  14. durationSeconds 运行时长(秒)
 *
 * + 扩展字段: device_ip, machine_status, alarm_code, alarm_message
 *
 * ======== 使用方式 ========
 *
 * # 本地测试（不需要MySQL连接，用内置测试数据）:
 * spark-submit --class org.example.tasks.ChangeDataCleaner \
 *   target/your-app.jar --mode test
 *
 * # 连接 MySQL 全量清洗并输出 CSV/JSON:
 * spark-submit --class org.example.tasks.ChangeDataCleaner \
 *   target/your-app.jar --mode full
 *
 * # 指定输出目录:
 * spark-submit --class org.example.tasks.ChangeDataCleaner \
 *   target/your-app.jar --output ./cleaned_data
 */
object ChangeDataCleaner {

  def main(args: Array[String]): Unit = {

    // ==========================================
    // 参数解析
    // ==========================================
    val mode = args.find(_ == "--mode").flatMap { m =>
      val idx = args.indexOf(m)
      if (idx + 1 < args.length) Some(args(idx + 1)) else None
    }.getOrElse("full")  // test | full

    val outputDir = args.find(_ == "--output").flatMap { o =>
      val idx = args.indexOf(o)
      if (idx + 1 < args.length) Some(args(idx + 1)) else None
    }.getOrElse(s"change_data_cleaned_${new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date)}")

    println("=" * 80)
    println("  ChangeDataCleaner - MySQL ChangeRecord 数据清洗工具")
    println("=" * 80)
    println(f"  模式: $mode%-8s | 输出目录: $outputDir")

    // ==========================================
    // 初始化 Spark
    // ==========================================
    val spark = SparkSession.builder()
      .appName("ChangeDataCleaner")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    var rawDF: DataFrame = null

    if (mode == "test") {
      // ========== TEST 模式：使用内置测试数据 ==========
      println("\n[TEST MODE] 使用内置测试数据（无需 MySQL 连接）...")

      val testXmls = Seq(
        // 样例1: 运行中、无报警
        """<col ColName="设备IP">192.168.1.201</col><col ColName="进给速度">0</col><col ColName="急停状态">否</col><col ColName="加工状态">MDI</col><col ColName="切削时间">495945</col><col ColName="循环时间">613424</col><col ColName="报警信息">{"AlmNo":0,"AlmStartTime":"无","AlmMsg":"无报警"}</col><col ColName="主轴转速">1200</col><col ColName="上电时间">1772023</col><col ColName="总加工个数">7850</col><col ColName="运行时间">770715</col><col ColName="主轴负载">45</col><col ColName="机器状态">运行</col><col ColName="连接状态">normal</col><col ColName="加工个数">3432</col>""",
        // 样例2: 待机状态
        """<col ColName="设备IP">192.168.1.204</col><col ColName="进给速度">0</col><col ColName="急停状态">否</col><col ColName="加工状态">MDI</col><col ColName="切削时间">518100</col><col ColName="循环时间">573760</col><col ColName="报警信息">{"AlmNo":0,"AlmStartTime":"无","AlmMsg":"无报警"}</col><col ColName="主轴转速">0</col><col ColName="上电时间">1799120</col><col ColName="总加工个数">7850</col><col ColName="运行时间">793215</col><col ColName="主轴负载">0</col><col ColName="机器状态">待机</col><col ColName="连接状态">normal</col><col ColName="加工个数">3432</col>""",
        // 样例3: 有报警
        """<col ColName="设备IP">192.168.1.202</col><col ColName="进给速度">200</col><col ColName="急停状态">否</col><col ColName="加工状态">自动运行</col><col ColName="切削时间">600000</col><col ColName="循环时间">650000</col><col ColName="报警信息">{"AlmNo":1021,"AlmStartTime":"2024-03-15 10:30:00","AlmMsg":"主轴过热"}</col><col ColName="主轴转速">3000</col><col ColName="上电时间">2000000</col><col ColName="总加工个数">9000</col><col ColName="运行时间">850000</col><col ColName="主轴负载">92</col><col ColName="机器状态">运行</col><col ColName="连接状态">normal</col><col ColName="加工个数">4000</col>""",
        // 样例4: 离线
        """<col ColName="设备IP">192.168.1.203</col><col ColName="进给速度">0</col><col ColName="急停状态">是</col><col ColName="加工状态">停止</col><col ColName="切削时间">100000</col><col ColName="循环时间">150000</col><col ColName="报警信息">{"AlmNo":0,"AlmStartTime":"无","AlmMsg":"无报警"}</col><col ColName="主轴转速">0</col><col ColName="上电时间">500000</col><col ColName="总加工个数">2000</col><col ColName="运行时间">200000</col><col ColName="主轴负载">0</col><col ColName="机器状态">异常停机</col><col ColName="连接状态">offline</col><col ColName="加工个数">500</col>"""
      )

      rawDF = testXmls.zipWithIndex.map { case (xml, i) =>
        val mid = f"machine_${i+1}%03d"
        (mid, xml, new java.sql.Timestamp(System.currentTimeMillis() + i * 60000L))
      }.toDF("machine_id", "change_data", "ts")

    } else {
      // ========== FULL 模式：从 MySQL 读取真实数据 ==========
      println("\n[FULL MODE] 从 MySQL 读取 ChangeRecord 表...")

      val mysqlUrl = AppConfig.getString(
        "mysql.jdbc.url",
        "jdbc:mysql://master:3306/shtd_industry?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8"
      )
      val mysqlUser = AppConfig.getString("mysql.user", "root")
      val mysqlPass = AppConfig.getString("mysql.password", "123456")
      val mysqlTable = AppConfig.getString("mysql.change_record_table", "ChangeRecord")

      println(s"[INFO] MySQL: $mysqlUrl / 表: $mysqlTable")

      val mysqlProps = new Properties()
      mysqlProps.put("user", mysqlUser)
      mysqlProps.put("password", mysqlPass)
      mysqlProps.put("driver", "com.mysql.cj.jdbc.Driver")

      rawDF = spark.read.jdbc(mysqlUrl, mysqlTable, mysqlProps)
      println(s"[INFO] 读取到 ${rawDF.count()} 条记录")
      rawDF.printSchema()

      // 自动检测 XML 列名和 ID 列名
      val xmlCol = rawDF.columns.find(c =>
        c.equalsIgnoreCase("ChangeData") || c.equalsIgnoreCase("change_data")
      ).getOrElse {
        println("[ERROR] 未找到 ChangeData 列! 表结构:")
        rawDF.columns.foreach(c => println(s"  - $c"))
        spark.stop()
        sys.exit(1)
      }

      // 构建标准 DataFrame
      val hasMachineId = rawDF.columns.exists(c => c.equalsIgnoreCase("machine_id"))

      rawDF = rawDF.withColumnRenamed(xmlCol, "change_data")
        .withColumn(
          "machine_id",
          when(lit(hasMachineId),
            coalesce(col("machine_id"), lit("unknown"))
          ).otherwise(
            ChangeDataXMLParser.udfDeviceIP(col("change_data"))
          )
        )

      // 尝试找时间戳列
      val tsCandidate = rawDF.columns.find(c =>
        c.equalsIgnoreCase("ts") || c.equalsIgnoreCase("timestamp") ||
          c.equalsIgnoreCase("create_time") || c.equalsIgnoreCase("update_time")
      )

      rawDF = tsCandidate match {
        case Some(tsCol) if tsCol != "ts" =>
          rawDF.withColumn("ts", col(tsCol).cast(TimestampType))
        case Some(_) => rawDF
        case None =>
          rawDF.withColumn("ts", current_timestamp())
      }
    }

    // ==========================================
    // 清洗：解析 XML → 提取 14 维特征
    // ==========================================
    println("\n[STEP 1] 解析 ChangeData XML，提取特征...")

    // 使用 ChangeDataXMLParser 批量解析
    val metricsDF = ChangeDataXMLParser.parseToMetricsDF(rawDF.select("machine_id", "ts", "change_data"))
    val stateDF = ChangeDataXMLParser.parseToStateDF(rawDF.select("machine_id", "ts", "change_data"))

    // 合并为一张宽表（包含所有14维+扩展字段）
    val cleanedDF = metricsDF.join(
      stateDF.select("machine_id", "ts", "isRunning", "isStandby", "isOffline",
        "isAlarm", "durationSeconds", "machineStatus", "alarmNo", "alarmMsg"),
      Seq("machine_id", "ts"),
      "inner"
    ).select(
      col("machine_id"),
      col("ts"),
      // ===== 14 维核心特征 =====
      col("isRunning").cast(DoubleType),
      col("isStandby").cast(DoubleType),
      col("isOffline").cast(DoubleType),
      col("isAlarm").cast(DoubleType),
      col("durationSeconds").cast(DoubleType),
      round(col("cuttingTime"), 2).alias("cuttingTime"),
      round(col("cycleTime"), 2).alias("cycleTime"),
      round(col("spindleLoad"), 2).alias("spindleLoad"),
      round(col("totalParts"), 2).alias("totalParts"),
      round(col("cumRuntime"), 2).alias("cumRuntime"),
      round(col("cumParts"), 2).alias("cumParts"),
      round(col("avgSpindleLoad"), 2).alias("avgSpindleLoad"),
      // ===== 扩展字段 =====
      round(col("spindleSpeed"), 2).alias("spindleSpeed"),
      round(col("feedSpeed"), 2).alias("feedSpeed"),
      col("device_ip"),
      col("machineStatus").alias("machine_status"),
      col("alarmNo").alias("alarm_code"),
      col("alarmMsg").alias("alarm_message")
    )

    val totalCount = cleanedDF.count()
    println(f"\n[RESULT] 清洗完成! 共 $totalCount%d 条记录，22 个字段")
    println("\n--- 数据预览（前20条）---")
    cleanedDF.show(20, false)

    // ==========================================
    // 数据质量报告
    // ==========================================
    println("\n" + "=" * 80)
    println("  数据质量报告")
    println("=" * 80)

    // 各状态的分布
    println("\n[状态分布]")
    cleanedDF.groupBy("machine_status")
      .count().orderBy(desc("count"))
      .show(false)

    // 报警统计
    println("\n[报警统计]")
    cleanedDF.groupBy("isAlarm")
      .count().show()

    // 设备IP去重数
    val distinctDevices = cleanedDF.agg(countDistinct("device_ip")).first().getLong(0)
    println(f"\n[设备覆盖] 去重后共 $distinctDevices%d 台不同设备")

    // 各指标的统计摘要
    println("\n[数值指标摘要]")
    cleanedDF.select(
      "cuttingTime", "cycleTime", "spindleLoad", "totalParts",
      "cumRuntime", "cumParts", "durationSeconds"
    ).summary("count", "min", "max", "mean", "stddev").show(false)

    // ==========================================
    // 输出文件
    // ==========================================
    println(s"\n[STEP 2] 写入文件到: $outputDir/")

    // 1) 完整宽表 CSV（推荐 Excel 直接打开）
    try {
      cleanedDF.coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(s"$outputDir/cleaned_full_csv")
      println(s"[OK] 完整宽表 CSV → $outputDir/cleaned_full_csv/")
    } catch { case e: Exception =>
      println(s"[WARN] CSV写入失败: ${e.getMessage}")
    }

    // 2) device_metrics 单独 CSV
    try {
      cleanedDF.select(
        "machine_id", "ts",
        "cuttingTime", "cycleTime", "spindleLoad", "totalParts",
        "cumRuntime", "cumParts", "avgSpindleLoad", "spindleSpeed", "feedSpeed", "device_ip"
      ).coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(s"$outputDir/device_metrics_csv")
      println(s"[OK] Metrics CSV → $outputDir/device_metrics_csv/")
    } catch { case e: Exception =>
      println(s"[WARN] Metrics CSV写入失败: ${e.getMessage}")
    }

    // 3) device_state 单独 CSV
    try {
      cleanedDF.select(
        "machine_id", "ts",
        "isRunning", "isStandby", "isOffline", "isAlarm",
        "durationSeconds", "machine_status", "device_ip", "alarm_code", "alarm_message"
      ).coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .option("header", "true")
        .option("encoding", "UTF-8")
        .csv(s"$outputDir/device_state_csv")
      println(s"[OK] State CSV → $outputDir/device_state_csv/")
    } catch { case e: Exception =>
      println(s"[WARN] State CSV写入失败: ${e.getMessage}")
    }

    // 4) JSON 备份（供后续 Flume/Kafka 使用）
    try {
      cleanedDF.coalesce(1)
        .write.mode(SaveMode.Overwrite)
        .json(s"$outputDir/json_backup")
      println(s"[OK] JSON备份 → $outputDir/json_backup/")
    } catch { case e: Exception =>
      println(s"[WARN] JSON写入失败: ${e.getMessage}")
    }

    // ==========================================
    // 完成
    // ==========================================
    println("\n" + "=" * 80)
    println(f"  ✅ 清洗完成! $totalCount%d 条记录已保存到 $outputDir/")
    println("=" * 80)
    println("""
    | 输出文件说明：
    |
    | 📁 cleaned_full_csv/     ← 完整宽表CSV（全部22字段，Excel直接打开）
    | 📁 device_metrics_csv/   ← 仅PLC数值指标（9个特征字段）
    | 📁 device_state_csv/     ← 仅设备状态字段（11个特征字段）
    | 📁 json_backup/          ← JSON格式（供Flume/Kafka下游消费）
    """.stripMargin)

    spark.stop()
  }
}
