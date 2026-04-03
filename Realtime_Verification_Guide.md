# 🎯 实时数据处理引擎 (Real-time Processing) 终极验证手册

本文档为“模块二：实时数据处理引擎”及其相关实战任务（任务 13-18）的详细验证指南。
在比赛评审环节，您可以通过本指南**分步骤、带预期结果**地向评委展示实时大屏和湖仓一体（Hudi）等技术点的成功落地。

---

## 🔧 零、验证前准备工作 (Pre-flight Checklist)

在进行所有验证之前，请确保以下基础设施正在运行：
1. **消息队列**：Kafka 及 Zookeeper 已启动，`sensor_topic` 和 `log_topic` 中有持续产生的数据。
2. **高速缓存**：Redis 已启动（`192.168.45.11:6379`），并**务必先运行**过一次 `InitRedisStandardParams.scala` 脚本以注入设备的标准工艺参数。
3. **数据仓库**：ClickHouse 已启动，目标数据库及数据表结构已建好。
4. **实时引擎**：启动主程序 `RealtimeEngine.scala`。

---

## 🖥️ 一、宏观运行状态验证 (Spark UI)

向评委展示引擎的“生命体征”和毫秒级处理能力，这是最直观的加分项。

### 1.1 访问 Spark UI
*   **操作步骤**：当 `RealtimeEngine` 在本地或集群启动后，打开浏览器访问 `http://localhost:4040` (如被占用则为 4041 等)。
*   **展示亮点**：
    *   进入 **Streaming** 选项卡，展示 **Input Rate** (每秒接入多少条数据) 曲线图，证明引擎正在毫秒级地消费 Kafka 数据。
    *   展示 **Processing Time** (处理延迟) 远低于批次间隔（例如处理只花了几十毫秒，而我们的触发器或微批是持续运行的），证明引擎**没有发生数据积压 (Lag)**。
    *   进入 **SQL** 选项卡，展示流式查询 (Streaming Queries) 的执行计划拓扑图。

---

## 🌊 二、任务组 4：实时数据入湖 (Flink/Spark -> Hudi)

此模块旨在展示“湖仓一体”架构中，如何通过 Hudi 实现高频流数据的实时 Upsert (更新/插入)。

### 📍 任务 13：实时传感器流写入 Hudi (MOR 表)
*   **核心逻辑**：将 `sensor_raw` 写入 `sensor_detail_realtime`，主键 `machine_id + ts`，开启异步压缩。
*   **验证步骤**：
    1. 保持 `RealtimeEngine` 持续运行。
    2. 运行专用的验证脚本 `TestReadHudiMOR.scala`（或者使用 Spark SQL 终端）。
*   **预期输出 (Console)**：
    ```text
    +----------+-------------------+-----------+-----------+...
    |machine_id|                 ts|temperature|vibration_x|...
    +----------+-------------------+-----------+-----------+...
    |       101|2026-04-03 10:00:05|       32.5|        1.1|...
    |       102|2026-04-03 10:00:04|       33.0|        0.9|...
    +----------+-------------------+-----------+-----------+...
    ```
*   **评委解说词**：“Hudi MOR 表通过 Snapshot 查询模式，将底层的 Parquet 基础数据和实时的 Log 增量数据进行了动态合并，您可以看到查询结果中包含了最新一秒钟注入的传感器数据，实现了离线与实时的统一。”

### 📍 任务 14：实时日志流写入 Hudi
*   **核心逻辑**：实时解析报错日志，打上紧急程度 (Critical/High/Normal) 标签。
*   **验证步骤**：在 Linux 终端查看 HDFS 的 Hudi Timeline。
    ```bash
    hadoop fs -ls /hudi/dwd/device_log_realtime/.hoodie | grep ".deltacommit"
    ```
*   **预期输出**：
    ```text
    -rw-r--r--  1 root supergroup    1452 2026-04-03 10:01 /hudi/dwd/.../.hoodie/20260403100105.deltacommit
    -rw-r--r--  1 root supergroup    1452 2026-04-03 10:02 /hudi/dwd/.../.hoodie/20260403100210.deltacommit
    ```
*   **评委解说词**：“随着实时流数据的摄入，Hudi 的时间线（Timeline）上不断产生新的 `.deltacommit` 记录，证明数据正在被毫秒级地持续写入数据湖中。”

---

## 📊 三、任务组 5：实时指标与推理 (Flink/Spark -> ClickHouse/Redis/Kafka)

此模块是整个比赛的核心看板数据源，包含了复杂的窗口计算、状态机匹配和机器学习预测。

### 📍 任务 15：实时状态窗口统计
*   **核心逻辑**：1分钟滚动窗口，计算最高温度、振动 RMS 均值等。
*   **验证步骤**：在 ClickHouse 中执行 SQL。
    ```sql
    SELECT window_end, machine_id, max_temperature, avg_vib_rms 
    FROM online_warehouse.realtime_status_window 
    ORDER BY window_end DESC LIMIT 5;
    ```
*   **预期输出**：
    | window_end          | machine_id | max_temperature | avg_vib_rms |
    |---------------------|------------|-----------------|-------------|
    | 2026-04-03 10:05:00 | 101        | 78.5            | 1.45        |

### 📍 任务 16 & 2.4：实时异常检测与报警 (多端输出)
*   **核心逻辑**：当传感器温度过高 ( > 80 ) 或日志报严重错误 (`Error Code 999`) 时触发双端报警。
*   **验证步骤 1 (Kafka 终端)**：
    ```bash
    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic alert_topic
    ```
    *   **预期输出**：看到 JSON 字符串流，包含 `Critical Log Error 999` 或 `High Temperature`。
*   **验证步骤 2 (ClickHouse 终端)**：
    ```sql
    SELECT alert_time, machine_id, alert_type, trigger_value 
    FROM online_warehouse.realtime_alerts 
    ORDER BY alert_time DESC LIMIT 5;
    ```
    *   **预期输出**：
    | alert_time          | machine_id | alert_type             | trigger_value |
    |---------------------|------------|------------------------|---------------|
    | 2026-04-03 10:12:33 | 104        | High Temperature       | 85.43         |
    | 2026-04-03 10:12:17 | 103        | Critical Log Error 999 | 999.0         |

### 📍 任务 17 & 2.4：实时 RUL 预测与大屏看板更新
*   **核心逻辑**：结合状态机（只在“稳定运行”下预测寿命），每条数据更新健康分并打入 ClickHouse。
*   **验证步骤 1 (ClickHouse 大屏看板)**：
    ```sql
    SELECT update_time, machine_id, machine_status, current_rul, health_score 
    FROM online_warehouse.device_realtime_status 
    ORDER BY update_time DESC LIMIT 5;
    ```
    *   **预期输出**：展示设备是否“异常停机”，以及 RUL 的实时计算结果（如 80.5 小时）。
*   **验证步骤 2 (Redis 快照)**：
    ```bash
    redis-cli
    HGETALL device_health
    ```
    *   **预期输出**：
    ```text
    1) "101"
    2) "95.0"
    3) "102"
    4) "60.0"
    ```
    *   **评委解说词**：“大屏端可以通过高速 Redis 接口以 O(1) 复杂度瞬间拉取全厂所有机器的最新健康评分，避免了关系型数据库的查询延迟。”

### 📍 任务 18：实时工艺参数偏离监控 (极限调优)
*   **核心逻辑**：对比传感器“电流/转速”与 Redis 中“标准参数”的偏差，当偏离率 > 2.8% 时报警写入 ClickHouse。
*   **验证步骤**：
    1. 确保 Kafka 模拟数据正在生成。
    2. 在 ClickHouse 中执行查询：
    ```sql
    SELECT record_time, machine_id, actual_value, standard_value, deviation_ratio 
    FROM online_warehouse.realtime_process_deviation 
    ORDER BY record_time DESC LIMIT 10;
    ```
*   **预期输出**：
    | record_time         | machine_id | actual_value | standard_value | deviation_ratio |
    |---------------------|------------|--------------|----------------|-----------------|
    | 2026-04-03 10:15:30 | 101        | 30.92        | 30.00          | 3.06            |
    | 2026-04-03 10:16:10 | 115        | 30.85        | 30.00          | 2.83            |
*   **评委解说词**：“这里我们运用了 Redis 广播维表 Join 技术。实际电流 30.92A 对比标准值 30.00A，偏离率为 3.06%，精准超过了我们设定的 2.8% 容忍度，从而触发了毫秒级的工艺偏离报警。数值全部经过 ROUND 处理，完美适配前端大屏的格式要求。”
