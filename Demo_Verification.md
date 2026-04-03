# 实时数据处理引擎 (Real-time Processing) 验证指南

本文档用于指导和验证模块二（实时数据处理引擎）中的所有任务场景是否正确实现。你可以通过手动往 Kafka 发送指定的 JSON 数据，并观察对应的输出结果来验证。

## ⚠️ 验证前置准备工作 (极其重要)

1. **清理环境**：在运行程序之前，务必清除本地 `C:\tmp\checkpoints\` 文件夹（如果是集群运行则清空 HDFS 对应的 checkpoint）。
2. **启动引擎**：启动 `org.example.tasks.RealtimeEngine`。
3. **清空测试表**（防止背景数据干扰）：
   ```sql
   TRUNCATE TABLE ldc.device_realtime_status;
   TRUNCATE TABLE ldc.realtime_status_window;
   TRUNCATE TABLE ldc.realtime_alerts;
   TRUNCATE TABLE ldc.realtime_process_deviation;
   ```
4. **关于查询的重要提示**：由于 ClickHouse 的 `ReplacingMergeTree` 去重是异步的，查询最新状态时**务必带上 `FINAL` 关键字**，例如：
   ```sql
   SELECT * FROM ldc.device_realtime_status FINAL LIMIT 20;
   ```

---

## 🔍 状态机逻辑解释

系统判定机器是“启动中”、“稳定运行”还是“异常停机”，**完全不看温度或电流**，而是严格遵循以下逻辑（见代码中的 `record_status` 和 `speed`）：
1. 如果设备从 Redis (`change_record_status`) 收到 `"预警"` 的状态标记，则直接判定为 **"异常停机"**。
2. 如果转速 (`speed`) **小于 1000**，则判定为 **"启动中"**。
3. 否则，无论温度多高（哪怕 85度、100度），只要转速正常且无预警，就一律判定为 **"稳定运行"**（高温会触发单独的 Alert 报警，但机器物理上仍在运行）。

---

## 🎯 验证场景列表

### 场景 1：高温报警 (关联任务 16)
* **目标**：验证当设备温度过高时，是否能够正确产生报警记录。
* **操作**：向 Kafka `sensor_raw` 发送以下 JSON：
  ```json
  {"machine_id":"101","ts":1775300000000,"temperature":85.5,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":30.0,"speed":3000.0}
  ```
* **预期效果**：
  * 查询 ClickHouse：`SELECT * FROM ldc.realtime_alerts`，会出现一条关于 101 的 "高温预警" 报警记录，建议措施为 "请立即检查设备"。

### 场景 2：日志错误 999 (关联任务 16 & 2.3)
* **目标**：验证日志异常检测机制（聚类/关键词），能否捕获 Error Code 999 突增并触发报警。
* **操作**：向 Kafka `log_raw` 发送以下 JSON：
  ```json
  {"machine_id":"103","ts":1775300000000,"error_code":"999","error_message":"检测到严重硬件故障","stack_trace":"java.lang.OutOfMemoryError..."}
  ```
* **预期效果**：
  * 查询 ClickHouse：`SELECT * FROM ldc.realtime_alerts`，会出现一条关于 103 的 "严重错误: 999" 报警，建议措施为 "请立即检查硬件日志"。
  * Kafka 的 `alert_topic` 中也能收到同样的报警消息。

### 场景 3：触发“启动中”状态 (关联任务 2.2 状态机 & 2.4)
* **目标**：验证当机器转速 `speed` < 1000 时，状态变为“启动中”。
* **操作**：向 Kafka `sensor_raw` 发送以下 JSON（注意 `speed` 为 500）：
  ```json
  {"machine_id":"104","ts":1775300000000,"temperature":45.0,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":30.0,"speed":500.0}
  ```
* **预期效果**：
  * 查询 ClickHouse：`SELECT * FROM ldc.device_realtime_status FINAL`，看到机器 104 的 `machine_status` 变为 **“启动中”**，且 `health_score` 降为 80。
  * Redis 中的 `device_health` hash 里 104 的分数也会更新。

### 场景 4：工艺参数偏离监控 (关联任务 18)
* **目标**：验证对比实时电流/转速与标准值偏差，记录偏差结果。
* **操作**：向 Kafka `sensor_raw` 发送以下 JSON（故意将 current 改为 35.0 制造偏离）：
  ```json
  {"machine_id":"105","ts":1775300000000,"temperature":45.0,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":35.0,"speed":3000.0}
  ```
* **预期效果**：
  * 查询 ClickHouse：`SELECT * FROM ldc.realtime_process_deviation`，会出现一条 105 的偏差记录。

### 场景 5：滑动窗口聚合统计 (关联任务 15)
* **目标**：验证 1分钟 滚动窗口的最大温度和振动均值统计。
* **操作**：只需让引擎自然消费数据超过 1 分钟即可触发 Watermark 滚动。
* **预期效果**：
  * 查询 ClickHouse：`SELECT * FROM ldc.realtime_status_window ORDER BY window_end DESC LIMIT 10`，能看到每分钟的聚合结果。
