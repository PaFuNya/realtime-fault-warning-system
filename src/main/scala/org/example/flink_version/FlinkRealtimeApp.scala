package org.example.flink_version

import org.apache.flink.streaming.api.scala._
import org.example.flink_version.tasks._

object FlinkRealtimeApp {

  def main(args: Array[String]): Unit = {
    // 1. 初始化 Flink 运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // 方便本地测试
    
    // 设置 Checkpoint (Hudi 需要 checkpoint 才能触发 commit)
    env.enableCheckpointing(5000)

    val ckUrl = "jdbc:clickhouse://127.0.0.1:8123/shtd_ads"

    // 2. 模块 2.1: 实时数据接入 (消费 Kafka)
    val sensorRawStream = Task2_1_DataIngestion.getSensorStream(env)
    val logRawStream = Task2_1_DataIngestion.getLogStream(env)

    // 3. 模块 2.2: 流式清洗填补缺失值、状态机识别、提取实时特征
    val enrichedStream = Task2_2_FeatureEngineering.getEnrichedStream(sensorRawStream)

    // 4. 模块 2.4: 状态快照 (更新 Redis) & 写入 ClickHouse 设备实时状态表
    Task2_4_RealtimeOutput.startSinks(enrichedStream, ckUrl)

    // 5. 任务 15: 实时状态窗口统计 (1分钟滚动窗口聚合写 ClickHouse)
    Task15_WindowStats.startWindowStatsSink(enrichedStream, ckUrl)

    // 6. 任务 16: 实时异常检测与报警 (推送至 ClickHouse & Kafka alert_topic)
    Task16_RealtimeAlerts.startAlertSinks(enrichedStream, logRawStream, ckUrl, Task2_1_DataIngestion.kafkaBrokers)

    // 7. 任务 18: 实时工艺参数偏离监控 (查询 Redis，持续偏离 30 秒报警)
    Task18_ProcessDeviation.startDeviationSink(enrichedStream, ckUrl)

    // 注意：任务 13 & 14 (Hudi Sink) 
    // 在 Flink 中写入 Hudi 推荐使用 Flink SQL API (Table API)，纯 DataStream 写入非常繁琐。
    // 为了保持 DataStream 风格统一，此处暂未混合 Table API。

    // 启动任务
    env.execute("Flink Realtime Data Process")
  }
}