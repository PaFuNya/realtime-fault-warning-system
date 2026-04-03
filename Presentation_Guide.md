# 🎤 实时数据处理引擎 - 15分钟答辩演示指南

各位评委老师好，我是今天答辩的同学。接下来，我将用约 15 分钟的时间，为您展示本项目中**模块二：实时数据处理引擎**的核心架构、代码实现，并进行实时的效果演示。

---

## 第一部分：整体架构与业务流程 (约 3 分钟)

本模块的核心目标是：**毫秒级感知异常、动态更新设备状态、即时报警**。

**数据流向（Pipeline）**：
1. **数据接入**：使用 Spark Structured Streaming 实时消费 Kafka 中的 `sensor_raw` (传感器物联网数据) 和 `log_raw` (设备运行日志)。
2. **实时计算**：在 Spark 引擎内部进行流式清洗、多时间粒度滑动窗口聚合、以及复杂状态机判定。
3. **多路输出**：
   * **ClickHouse**：将实时指标和窗口聚合数据写入 `ReplacingMergeTree` 表，供大屏毫秒级查询。
   * **Redis**：维护设备最新健康分（Health Score）快照，供其他微服务快速点查。
   * **Kafka**：一旦触发规则，将报警信息实时反写回 Kafka 的 `alert_topic`。

> 💡 **架构亮点（向评委强调的加分项）**：
> 在开发过程中，我们发现如果将 Hudi 的实时入湖（MOR表异步压缩）与毫秒级预警放在同一个 Spark 任务中，Hudi 的重量级 I/O 会导致微批次处理延迟飙升至 5 分钟以上。
> **我们的解决方案**：为了保证“毫秒级预警”的核心诉求，我们在答辩版本中将 Hudi 的写入剥离（通过微服务拆分思想），确保了 ClickHouse 和 Redis 的响应延迟控制在亚秒级。

---

## 第二部分：核心代码解析 (约 5 分钟)

这是本引擎最核心的三段代码逻辑，体现了我们对流处理难点的解决：

### 1. 状态机与维度表关联查找 (打破 Watermark 限制)
在流处理中，判断设备是“启动中”还是“异常停机”，需要结合 Redis 中的指令流。如果直接使用 Spark 的流-流 Join (`leftOuter`)，会因为一边数据迟迟不来导致 Watermark 死锁，数据全部卡住。
**我们的解法**：使用 `mapPartitions` 结合 Jedis 客户端，在每个 Executor 上**异步反查 Redis 维度表**。

```scala
// 核心代码：基于 Redis 旁路查找的设备状态机流转
val withStatusDF = batchDF.mapPartitions { iter =>
  val jedis = new Jedis("bigdata1", 6379) // 建立复用的连接
  val res = iter.map { row =>
    val mid = row.getAs[String]("machine_id")
    val speed = row.getAs[Double]("speed")
    val recordStatus = jedis.hget("change_record_status", mid) // 从 Redis 拿预警指令
    
    // 状态判定逻辑：预警最高优 -> 转速 < 1000 判定启动 -> 其他为稳定运行
    val machineStatus = if (recordStatus == "预警") "异常停机"
                        else if (speed < 1000) "启动中"
                        else "稳定运行"
    // ... 更新并返回新的 Row
  }
  jedis.close()
  res
}
```

### 2. 双流联合报警检测 (规则引擎 + 日志模式匹配)
不仅监控物理传感器（高温），同时监控软件日志（严重错误 999），最后将双流的报警结果通过 `unionByName` 合并。

```scala
// 核心代码：传感器异常判定
val sensorAlerts = sensorRawDF
  .filter(col("temperature") > 80.0)
  .select(col("machine_id"), col("timestamp"), lit("高温预警").alias("alert_type"))

// 核心代码：日志异常判定
val logAlerts = logDF
  .filter(col("error_code") === "999")
  .select(col("machine_id"), col("timestamp"), lit("严重错误: 999").alias("alert_type"))

// 核心代码：合并两路报警，统一推送 Kafka 和 ClickHouse
val combinedAlerts = sensorAlerts.unionByName(logAlerts)
```

### 3. 多重时间窗口并发聚合 (突破 Spark 限制)
Spark 不允许在同一个 Streaming DataFrame 上进行多次 `groupBy`。
**我们的解法**：将数据源缓存或分流，使用单独的 `writeStream` 独立计算 1分钟、5分钟、1小时 的滑动窗口聚合。

```scala
// 核心代码：1分钟滚动窗口聚合（最大温度、平均振动）
val window1mDF = sensorRawDF
  .withWatermark("timestamp", "1 seconds")
  .groupBy(window(col("timestamp"), "1 minute"), col("machine_id"))
  .agg(
    max("temperature").alias("max_temperature"),
    avg("vibration_x").alias("avg_vib_rms") // 简化公式
  )
```

---

## 第三部分：实弹测试与效果演示 (约 7 分钟)

请评委老师看屏幕，我现在将通过手动注入极端数据，来验证系统的毫秒级响应。

*(提前执行：`TRUNCATE TABLE ldc.device_realtime_status; TRUNCATE TABLE ldc.realtime_alerts;`)*

### 🎯 演示场景 1：状态流转为“启动中”并扣减健康分
**操作**：模拟 104 号机器刚通电，转速（speed）只有 500。
*向 Kafka 发送：*
```json
{"machine_id":"104","ts":1775300000000,"temperature":45.0,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":30.0,"speed":500.0}
```
**验证**：在 ClickHouse 中立刻执行查询（强调 `FINAL` 关键字解决异步去重问题）：
```sql
SELECT machine_id, machine_status, health_score FROM ldc.device_realtime_status FINAL WHERE machine_id='104';
```
> **结果展示**：`machine_status` 瞬间变为 "启动中"，`health_score` 降为 80 分。

### 🎯 演示场景 2：物理高温与软件崩溃双重报警
**操作**：模拟 101 机器温度飙升至 85度，同时 103 机器底层软件抛出 Error 999。
*向 Kafka 发送传感器数据：*
```json
{"machine_id":"101","ts":1775300000000,"temperature":85.5,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":30.0,"speed":3000.0}
```
*向 Kafka 发送日志数据：*
```json
{"machine_id":"103","ts":1775300000000,"error_code":"999","error_message":"检测到严重硬件故障","stack_trace":"java.lang.OutOfMemoryError"}
```
**验证**：在 ClickHouse 中查询报警表：
```sql
SELECT alert_time, machine_id, alert_type, suggested_action FROM ldc.realtime_alerts;
```
> **结果展示**：同时出现 101 的“高温预警”和 103 的“严重错误: 999”，并且 Kafka 的 `alert_topic` 控制台也同步输出了警告消息。

---

## 总结

综上所述，我们的实时数据处理引擎：
1. **快**：通过 Redis 旁路和剥离重量级 I/O，实现了真正的毫秒级延迟。
2. **准**：严格基于状态机逻辑，结合多数据源（日志+传感器）交叉验证。
3. **稳**：妥善处理了 Watermark 乱序、ClickHouse 的异步去重，保证了大屏指标的准确无误。

我的代码展示和演示到此结束，感谢评委老师！