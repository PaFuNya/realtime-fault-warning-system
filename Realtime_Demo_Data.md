# 🎯 实时引擎终极演示 (Demo) 专用触发数据

在比赛演示或答辩环节，您可以按照本指南的步骤：
1. 清空 ClickHouse 相关表。
2. 将特定的 JSON 数据发送至对应的 Kafka Topic。
3. 观察 ClickHouse 数据表的实时更新。

---

## 🧹 步骤一：清空测试数据表

在 ClickHouse 客户端 (DBeaver/终端) 中执行以下语句，确保演示环境干净：

```sql
-- 清空大屏状态表
TRUNCATE TABLE device_realtime_status;
-- 清空报警记录表
TRUNCATE TABLE realtime_alerts;
-- 清空工艺偏离表
TRUNCATE TABLE realtime_process_deviation;
```

---

## 🚀 步骤二：发送 JSON 触发特定任务

请打开两个终端窗口，分别启动 `sensor_raw` 和 `log_raw` 的 Kafka 生产者：

**终端 A (发送传感器数据)**：
```bash
kafka-console-producer.sh --broker-list localhost:9092 --topic sensor_raw
```

**终端 B (发送日志数据)**：
```bash
kafka-console-producer.sh --broker-list localhost:9092 --topic log_raw
```

---

### 📍 场景 1：触发“正常稳定运行”及健康分更新 (关联 2.4 & 17)

**目标**：在大屏表 (`device_realtime_status`) 中生成一条正常数据，触发 RUL 预测，并在 Redis 中更新高健康分。

**操作**：向 **终端 A (sensor_raw)** 发送以下正常数据：

```json
{"machine_id":"101","ts":1775188800000,"temperature":45.0,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":30.0,"speed":3000.0}
```

**预期效果**：
1. ClickHouse 查询 `device_realtime_status` 会出现机器 101，状态为“稳定运行”，`current_rul` 有具体预测值，`health_score` 为 95.0。
2. Redis 中 `device_health` 会更新 `101` 的分数为 95.0。

---

### 📍 场景 2：触发“高温报警” (关联 16 & alert_topic)

**目标**：触发传感器温度 > 80 的报警规则。

**操作**：向 **终端 A (sensor_raw)** 发送以下高温数据（温度 85.5）：
```json
{"machine_id":"102","ts":1775188810000,"temperature":85.5,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":30.0,"speed":3000.0}
```

**预期效果**：

1. ClickHouse 查询 `realtime_alerts` 会立即出现机器 102，`alert_type` 为 `High Temperature`，触发值为 85.5。
2. Kafka 消费者监听 `alert_topic` 会收到该 JSON 报警消息。

---

### 📍 场景 3：触发“严重日志错误 999” (关联 16 & alert_topic)

**目标**：触发设备出现致命错误 999 的报警规则。

**操作**：向 **终端 B (log_raw)** 发送以下严重日志数据：
```json
{"machine_id":"103","ts":1775188820000,"error_code":"999","error_message":"Fatal System Crash","stack_trace":"java.lang.OutOfMemoryError..."}
```

**预期效果**：
1. ClickHouse 查询 `realtime_alerts` 会立即出现机器 103，`alert_type` 为 `Critical Log Error 999`。
2. Kafka 消费者监听 `alert_topic` 会收到该 JSON 报警消息。

---

### 📍 场景 4：触发“异常停机”状态 (关联 2.2 状态机 & 2.4)

**目标**：触发机器转速极低（< 1000）导致状态变为非稳定运行，并降低健康分。
*注意：状态机的计算和 `device_realtime_status` 的输出是基于数据批次流动的，请确保此时至少有一条匹配的 ChangeRecord，或者为了演示简便，连续发送 2 条数据以推动流转。*

**操作**：向 **终端 A (sensor_raw)** 连续发送以下低转速数据 2 次：
```json
{"machine_id":"104","ts":1775188830000,"temperature":45.0,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":30.0,"speed":500.0}
{"machine_id":"104","ts":1775188832000,"temperature":45.0,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":30.0,"speed":500.0}
```

**预期效果**：
1. ClickHouse 查询 `device_realtime_status` 会出现机器 104，`machine_status` 变为“启动中”或“异常停机”，且 `health_score` 降为 60.0。
2. Redis 中 `device_health` 会更新 `104` 的分数为 60.0。

---

### 📍 场景 5：触发“工艺参数偏离报警” (关联 18)

**目标**：触发电流或转速严重偏离 Redis 标准值（偏离率 > 2.8%）。
*前提：已确保 Redis 中 105 号机器的标准电流为 30.0。*
*关键：任务 18 的 SQL 中使用了 `HAVING count(*) >= 2`（或者滑动窗口机制需要多条数据结算），这意味着你必须在同一个窗口内（10秒内）连续发送至少 2 条异常数据才能触发写入！*

**操作**：向 **终端 A (sensor_raw)** **连续发送以下高电流数据 2-3 次**（中间不要停顿太久）：

```json
{"machine_id":"105","ts":1775188840000,"temperature":45.0,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":35.0,"speed":3000.0}
{"machine_id":"105","ts":1775188842000,"temperature":45.0,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":35.0,"speed":3000.0}
```

**预期效果**：
1. ClickHouse 查询 `realtime_process_deviation` 会在约 10 秒（滑动窗口结算）后出现机器 105。
2. `actual_value` 为 35.00，`standard_value` 为 30.00，`deviation_ratio` 约为 16.67%。