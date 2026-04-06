# ChangeDataCleaner 使用指南

> 从 MySQL `shtd_industry.ChangeRecord` 表清洗 881 行 XML 数据 → 输出结构化 CSV/JSON

---

## 一、数据源

**表：** `shtd_industry.ChangeRecord`
**字段：** `ChangeData` (XML 类型)
**记录数：** 881 行

### 原始 XML 示例

```xml
<col ColName="设备IP">192.168.1.201</col>
<col ColName="切削时间">495945</col>
<col ColName="循环时间">613424</col>
<col ColName="报警信息">{"AlmNo":0,"AlmMsg":"无报警"}</col>
<col ColName="主轴负载">0</col>
<col ColName="机器状态">运行</col>
...
```

## 二、输出结果（14 维特征）

| # | 字段名 | 中文名 | 类型 | 来源 | 示例值 |
|---|--------|--------|------|------|--------|
| 1 | **isRunning** | 运行中 | double(0/1) | 机器状态=运行 | 1.0 |
| 2 | **isStandby** | 待机 | double(0/1) | 机器状态=待机/MDI | 0.0 |
| 3 | **isOffline** | 离线 | double(0/1) | 连接状态≠normal | 0.0 |
| 4 | **isAlarm** | 报警 | double(0/1) | AlmNo > 0 | 0.0 |
| 5 | **durationSeconds** | 运行时长(秒) | double | 运行时间字段 | 770715.0 |
| 6 | **cuttingTime** | 切削时间(秒) | double | 切削时间 | 495945.0 |
| 7 | **cycleTime** | 循环时间(秒) | double | 循环时间 | 613424.0 |
| 8 | **spindleLoad** | 主轴负载(%) | double | 主轴负载 | 0.0 |
| 9 | **totalParts** | 总加工个数 | double | 总加工个数 | 7850.0 |
| 10 | **cumRuntime** | 上电累计(秒) | double | 上电时间 | 1772023.0 |
| 11 | **cumParts** | 本次加工数 | double | 加工个数 | 3432.0 |
| 12 | **avgSpindleLoad** | 负载均值(%) | double | ≈主轴负载 | 0.0 |
| 13 | **spindleSpeed** | 主轴转速(RPM) | double | 主轴转速 | 0.0 |
| 14 | **feedSpeed** | 进给速度 | double | 进给速度 | 0.0 |

+ 扩展字段：`device_ip`, `machine_status`, `alarm_code`, `alarm_message`

## 三、文件清单

| 文件 | 用途 |
|------|------|
| `ChangeDataXMLParser.scala` | XML 解析引擎（UDF + 批量解析） |
| `ChangeDataCleaner.scala` | **清洗主程序**（读MySQL→解析→输出CSV） |
| `config.properties` | 配置（MySQL地址/密码等） |

## 四、使用方式

### 方式 A：本地测试（不需要 MySQL）

```bash
# IDEA 里直接运行 ChangeDataCleaner 的 main 方法，参数填: --mode test
# 或者命令行：
spark-submit --class org.example.tasks.ChangeDataCleaner \
  target/your-app.jar --mode test --output ./test_output
```

这会使用 4 条内置测试数据（运行/待机/报警/离线各一条），验证解析逻辑是否正确。

### 方式 B：服务器上全量清洗（连接 MySQL）

```bash
# 1. 打包
mvn clean package -DskipTests

# 2. 上传到服务器后执行：
spark-submit --class org.example.tasks.ChangeDataCleaner \
  --master yarn \
  your-app.jar --mode full --output /data/change_data_cleaned
```

### 方式 C：只运行单元测试（验证解析逻辑）

```bash
spark-submit --class org.example.tasks.ChangeDataXMLParser \
  target/your-app.jar
```

## 五、输出文件目录

执行后生成以下目录：

```
change_data_cleaned_20260405_192800/
├── cleaned_full_csv/        ← 完整宽表CSV（22列，Excel直接打开）
│   └── part-00000-.csv      ← ★ 主要看这个文件
├── device_metrics_csv/      ← 仅PLC数值指标（12列）
│   └── part-00000-.csv
├── device_state_csv/        ← 仅设备状态（11列）
│   └── part-00000-.csv
└── json_backup/             ← JSON格式（供Flume/Kafka下游消费）
    └── part-00000-.json
```

## 六、配置修改

编辑 `src/main/resources/config.properties`：

```properties
# MySQL 连接信息（改成你服务器的实际地址和密码）
mysql.jdbc.url=jdbc:mysql://master:3306/shtd_industry?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8
mysql.user=root
mysql.password=你的密码
mysql.change_record_table=ChangeRecord
```

## 七、清洗规则详解

### 状态映射（machineStatus → isRunning/isStandby）

| 原始机器状态 | isRunning | isStandby |
|-------------|-----------|-----------|
| 运行 / 运行中 / 自动运行 / AUTO | **1.0** | 0.0 |
| 待机 / 暂停 / RESET / MDI | 0.0 | **1.0** |
| 其他（异常停机/未知） | 0.0 | 0.0 |

### 报警判断

从 `<col ColName="报警信息">` 中提取 JSON：
- `"AlmNo": 0` → isAlarm = 0.0（无报警）
- `"AlmNo": 1021` → isAlarm = 1.0（有报警）
- 同时提取 alarm_code 和 alarm_message

### 数值清洗

- 所有数值字段自动去除非数字字符（中文、单位等）
- 无法转换的默认为 0.0
- 保留 2 位小数精度
