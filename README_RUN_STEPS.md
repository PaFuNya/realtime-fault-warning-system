# 实时数据处理引擎：运行步骤与需求说明

本项目是一个基于 Lambda 架构 Speed Layer 的工业物联网（IoT）实时数据处理引擎。主要处理从 Kafka 接入的设备传感器数据和日志数据，进行实时特征计算、异常检测报警，并将结果落库至 ClickHouse 和 Hudi。

---

## 🏗️ 架构流程与代码映射 (ODS -> DWD -> DWS -> ADS)

我们的代码架构完全契合了工业界标准的实时数仓分层理论。具体映射关系如下：

### 1. 【ODS层 / 缓冲层】 (消息队列：Kafka)
*   **定位**：原汁原味的原始数据流。
*   **代码映射**：`SparkUtils.scala` 中的 `getSensorRawStream` 和 `getLogRawStream` 方法。
*   **实现**：使用 Spark 消费 Kafka 的 `sensor_raw` 和 `log_raw` 主题，并执行流式清洗（JSON解析、过滤、类型转换）。

### 2. 【DWD层】 (数据湖底层存储：Hudi)
*   **定位**：清洗干净的底层明细流水表。
*   **代码映射**：`Task13_14_HudiSink.scala`
*   **实现**：将清洗后的传感器明细和设备日志明细，实时 Upsert 写入 HDFS 上的 Hudi MOR 表（`/hudi/dwd/sensor_detail_realtime` 和 `/hudi/dwd/device_log_realtime`）。

### 3. 【DWS层】 (分析引擎：ClickHouse)
*   **定位**：基于时间窗口的聚合统计指标，供业务查询。
*   **代码映射**：`Task15_WindowStats.scala`
*   **实现**：基于 ODS/DWD 清洗后的数据流，开启 1 分钟滚动窗口，计算最大温度、振动 RMS 均值等，并写入 ClickHouse 的状态窗口统计表 `shtd_ads.realtime_status_window`。

### 4. 【ADS层】 (业务应用终端：ClickHouse & Kafka)
*   **定位**：直接供给前端大屏展示、预警系统的最终结果。
*   **代码映射**：
    *   `Task16_RealtimeAlerts.scala`：计算温度/振动超标报警，写入 ClickHouse 异常预警表 `shtd_ads.realtime_alerts`。
    *   `Task18_ProcessDeviation.scala`：计算对比标准值的工艺偏离记录，写入 ClickHouse 工艺偏离表。
    *   *(注：后续的 RUL 预测和报警推送 Kafka 等需求，也将在此层平行扩展)*

> **核心优化提示**：为了保证工业物联网毫秒级的实时预警，我们在物理执行上采用了 **并行计算** 的架构。即 DWD 慢慢入湖（Task 13/14）与 DWS/ADS 极速计算（Task 15/16/18）同时从 ODS 层独立读取数据，完美避免了 Hudi 数据湖分钟级 Commit 带来的报警延迟问题。

---

## 🚀 模块与任务需求映射说明

为了避免重复消费 Kafka 造成资源浪费，我们**没有**为每一个小需求单独建立运行文件。很多基础需求（如 2.1 数据接入、2.4 结果输出）被抽象封装成了工具类或作为动作（Action）嵌入到了具体的计算任务中。

### 📌 需求 2.1：实时数据接入
*   **需求**：消费 Kafka `sensor_raw` 和 `log_raw` 主题，解析 JSON，提取字段。
*   **运行说明**：**不需要单独运行！** 它被封装在 `SparkUtils.getSensorRawStream` 等方法中。每当你启动任何一个后续的 Task（如 15, 16），它们在启动时第一秒就会自动调用该模块完成数据的接入和清洗。

### 📌 需求 2.4：实时结果输出
*   **需求**：报警消息推送至 Kafka `alert_topic`；实时指标写入 ClickHouse `device_realtime_status` 表；状态快照更新 Redis。
*   **运行说明**：**不需要单独运行！** 它代表的是计算任务的“出口”。例如，任务 16 计算出报警后，其代码的后半段会自动触发写入 ClickHouse 和推送 Kafka 的动作。它已经融入到了各个具体 Task 的 `writeStream` 逻辑中。

---

## 🛠️ 完整环境启动与运行步骤

请严格按照以下阶段顺序操作，以确保数据流的完整闭环。

### 阶段一：启动底层基础设施 (Linux 服务器)

1.  **启动 Zookeeper & Kafka 集群**：
    确保三台节点（bigdata1, bigdata2, bigdata3）的 Kafka 服务正常运行。
    *检查命令*：`jps` 应该能看到 `Kafka` 和 `QuorumPeerMain` 进程。
2.  **启动 ClickHouse 数据库**：
    在 ClickHouse 所在节点运行启动命令：
    ```bash
    sudo -u clickhouse clickhouse-server --config-file=/etc/clickhouse-server/config.xml --daemon
    ```
3.  **启动 Redis 服务**：
    确保 Redis 运行在 `127.0.0.1:6379`（用于后续的工艺参数对比和状态快照存储）。

### 阶段二：开启模拟数据源 (Linux 服务器)

在 `bigdata1` 服务器上，启动数据生成脚本，让其源源不断地向 Kafka 的 `sensor_raw` 和 `log_raw` topic 发送数据。
```bash
sh generate_data.sh
```
*(注意：请保持该脚本在后台运行，因为流计算需要持续的时间戳推动水位线 Watermark 的前进。)*

### 阶段三：运行 Spark Streaming 任务 (Windows/IDEA 开发环境)

在确保 Windows `hosts` 文件已正确配置集群 IP 映射的前提下，按照以下顺序启动 Scala 任务（可以通过 IDE 的 Run 按钮，或使用 Maven 命令行）：

#### 第 1 步：验证全链路连通性 —— 运行 Task 16
*   **运行文件**：`Task16_RealtimeAlerts.scala`
*   **任务逻辑**：实时异常检测与报警（温度 > 80 OR 振动突增；日志报 Error 999）。
*   **预期结果**：运行后 10 秒左右，使用 ClickHouse 客户端查询：
    ```sql
    SELECT * FROM ldc.realtime_alerts;
    ```
    如果能查到带有 `High Temperature` 或 `Critical Log Error 999` 的数据，说明 **Kafka -> Spark -> ClickHouse** 的全链路已经完全打通！

#### 第 2 步：验证时间窗口聚合 —— 运行 Task 15
*   **运行文件**：`Task15_WindowStats.scala`
*   **任务逻辑**：1 分钟滚动窗口，计算各设备温度最大值、振动 RMS 均值。
*   **预期结果**：因为我们配置了 `Update` 输出模式和极短的水位线（5 seconds），运行后几乎立刻就能在 ClickHouse 中查到持续更新的聚合数据：
    ```sql
    SELECT * FROM ldc.realtime_status_window;
    ```
    *(注：如果报错找不到文件，请先删除本地磁盘如 `C:\tmp\checkpoints\status_window_standalone` 下的历史 Checkpoint 文件夹)*

#### 第 3 步：数据入湖存档 —— 运行 Task 13 & 14
*   **运行文件**：`Task13_14_HudiSink.scala`
*   **任务逻辑**：将原始传感器数据和日志数据（附带紧急程度标记）以 MOR (Merge-On-Read) 格式实时 Upsert 进 Hudi 数据湖，并开启异步压缩。
*   **预期结果**：不会有控制台打印。请前往目标文件系统 HDFS 对应目录，查看是否生成了 `.parquet` 和 `.log` 文件：
    ```bash
    hadoop fs -ls /hudi/dwd/sensor_detail_realtime
    hadoop fs -ls /hudi/dwd/device_log_realtime
    ```

#### 🏆 终极挑战：运行整合大引擎 —— RealtimeEngine
*   **运行文件**：`RealtimeEngine.scala`
*   **说明**：如果你前面的单任务都能跑通，说明逻辑完全正确。这个文件将 2.1、2.2、2.4、任务 15、16、18 **全部整合在一起**。
*   **优势**：只需读取一次 Kafka 流，即可分发到多条业务线（写 ClickHouse 窗口表、写报警表、写大屏展示表、更新 Redis 快照、推送 Kafka 报警）。这是企业级生产环境中最节省资源、最高效的标准运行模式。

---

## ⚠️ 常见排错指南

1. **`Connection to node X could not be established`**
   - **原因**：Windows 无法解析 Kafka 集群的主机名。
   - **解决**：修改 Windows 的 `C:\Windows\System32\drivers\etc\hosts` 文件，添加 `192.168.45.11 bigdata1` 等真实 IP 映射。
2. **表里迟迟没有数据**
   - **原因**：模拟数据生成停止，导致 Spark 的 Watermark（水位线）无法向前推进，窗口无法闭合。
   - **解决**：确保 `generate_data.sh` 在持续发数据。
3. **报错 `java.io.FileNotFoundException: ... .delta does not exist`**
   - **原因**：修改了代码逻辑或输出模式，但旧的 Checkpoint 进度文件还在。
   - **解决**：去 `/tmp/checkpoints/` 目录下删掉对应的旧检查点文件夹，重新运行。