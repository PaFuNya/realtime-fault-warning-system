# 任务与代码映射说明

由于项目要求严格对应《Match.docx》中的各个模块与任务，为了保持代码的清晰性、可维护性，我们使用了 **Spark Scala** 进行了重构，并将原先的单文件应用 (`SparkRealtimeApp.scala`) 拆分为了多个专门的 Task 对象。

> **注意：** 项目中依然保留了最初的 Java Flink 源代码（位于 `src/main/java/org/example/` 下）。但因为 `pom.xml` 中去除了 Flink 依赖转而使用了 Spark，这些 Java 文件会导致编译报错。它们仅作为历史参考保留。实际运行的是 `src/main/scala` 下的 Spark 代码。

---

## 模块二：实时数据处理引擎

### 2.1 实时数据接入 & 流式清洗
- **需求**：消费 Kafka `sensor_raw` 和 `log_raw`；解析 JSON；填补短时缺失值（线性插值）。
- **实现位置**：
  - 数据接入与 JSON 解析：[Task2_1_DataIngestion.scala](src/main/scala/org/example/tasks/Task2_1_DataIngestion.scala)
  - 缺失值填补（前向插值替代）：[Task2_2_FeatureEngineering.scala](src/main/scala/org/example/tasks/Task2_2_FeatureEngineering.scala) 中的 `updateDeviceState` 方法。

### 2.2 实时特征计算 (Streaming Features)
- **需求**：计算温度上升斜率、振动突增率；状态机识别（启动中/稳定运行/异常停机）。
- **实现位置**：[Task2_2_FeatureEngineering.scala](src/main/scala/org/example/tasks/Task2_2_FeatureEngineering.scala)
  - 利用 Spark 的 `flatMapGroupsWithState` 维护设备的最新状态，计算斜率与突增率，并根据 `speed` 字段实现状态机切换。

### 2.3 实时推理 (Real-time Inference)
- **需求**：加载 LightGBM 模型预测 RUL (剩余使用寿命)。
- **实现位置**：*由于题目说明“17可以先不做”，此模块暂未实现具体代码。*

### 2.4 实时结果输出
- **需求**：推送报警到 Kafka `alert_topic`；实时指标写入 ClickHouse；状态快照每分钟更新 Redis。
- **实现位置**：
  - **Redis 状态快照 & ClickHouse 指标**：[Task2_4_RealtimeOutput.scala](src/main/scala/org/example/tasks/Task2_4_RealtimeOutput.scala)
  - **Kafka 报警推送**：[Task16_RealtimeAlerts.scala](src/main/scala/org/example/tasks/Task16_RealtimeAlerts.scala)

---

## 三、实时数据处理任务 (Hudi + ClickHouse)

### 任务组 4：实时数据入湖
- **13. 实时传感器流写入 Hudi** & **14. 实时日志流写入 Hudi**
- **需求**：写入 Hudi。**注意**：根据最新要求，已放弃使用 MOR 表，改为使用正常的 COW (COPY_ON_WRITE) 表。
- **实现位置**：[Task13_14_HudiSink.scala](src/main/scala/org/example/tasks/Task13_14_HudiSink.scala)

### 任务组 5：实时指标与推理
- **15. 实时状态窗口统计**
  - **需求**：1 分钟滚动窗口，计算最大温度和振动 RMS 均值，写入 ClickHouse。
  - **实现位置**：[Task15_WindowStats.scala](src/main/scala/org/example/tasks/Task15_WindowStats.scala)

- **16. 实时异常检测与报警**
  - **需求**：结合传感器异常（温度/振动）和日志异常（Error 999），写入 ClickHouse 并推送 Kafka。
  - **实现位置**：[Task16_RealtimeAlerts.scala](src/main/scala/org/example/tasks/Task16_RealtimeAlerts.scala)

- **17. 实时 RUL 动态预测**
  - **说明**：暂不实现（遵循要求）。

- **18. 实时工艺参数偏离监控**
  - **需求**：对比实时电流/转速与 **Redis 中存储的标准值** 的偏差，持续偏离 30 秒即记录到 ClickHouse。
  - **实现位置**：[Task18_ProcessDeviation.scala](src/main/scala/org/example/tasks/Task18_ProcessDeviation.scala)

---

## 统一启动类
所有的任务都被统一注册并在 [SparkRealtimeApp.scala](src/main/scala/org/example/SparkRealtimeApp.scala) 的 `main` 方法中启动。该文件作为应用程序的入口。