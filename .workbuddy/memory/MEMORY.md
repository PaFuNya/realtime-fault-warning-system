# 长期记忆 - MATCH OverMatch 项目

## 项目概述
CNC机床实时数据处理平台：Flume → Kafka → Flink(特征计算+RUL预测) → 报警输出

## 服务器信息
- **内网IP**: 192.168.45.100 (用户名: root, 密码: 123456)
- **组件位置**: /opt/module/ 下有 flume, kafka, flink, hadoop, jdk
- **数据目录**: /data_log/ (日志文件生成位置)

## 关键配置
- **Flume**: MyFlume.conf (6个source: 3旧+3新), 监控 /data_log/*.log
- **Flink Web UI端口**: 改为8090（8081被Hadoop占用）
- **JAR包**: realtime-data-process-1.0-SNAPSHOT.jar (~394MB)
- **主类**: org.example.tasks.FlinkRulInference (pom.xml已配Main-Class)

## 数据流
```
数据生成脚本(/opt/module/flume/conf/*.sh) 
  → /data_log/*_generated.log 
  → Flume(MyFlume.conf, tail -F) 
  → Kafka Topics(device_state/highfreq_sensor/sensor_metrics) 
  → Flink(Flink-FeatureEngine-v2)
  → 窗口聚合(3min TumblingWindow): var_temp/kurtosis_temp/fft_peak等7个特征
  → 报警输出: warning_log topic
```

## 已验证功能 (2026-04-07)
- ✅ 全链路运行正常
- ✅ var_temp计算: 各设备均有输出(范围16~71)
- ✅ kurtosis_temp + fft_peak + avg_spindle_load 等7个特征
- ✅ warning_log报警topic持续写入(~1508条)

## 注意事项
- Flink提交用 `bin/flink run -d` detached模式，避免REST超时
- 服务器时间比实际慢约8小时
- 9台设备编号: 109-117
