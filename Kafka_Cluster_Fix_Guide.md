# Kafka 3-Node Cluster Fix Guide

## 问题诊断结果

### 根本原因
```
ERROR Shutdown broker because all log dirs in /opt/module/kafka/data have failed
Suppressed: java.nio.file.AtomicMoveNotSupportedException
```

**Kafka 数据目录损坏**，导致所有 broker 无法启动。

### 已修复的问题
| 问题 | 状态 |
|------|------|
| /etc/hosts 不完整 | ✅ 已修复（3节点互相可见） |
| Kafka listeners 配置 | ✅ 已修复（0.0.0.0:9092 + 主机名:9092） |
| Kafka 数据目录损坏 | ⚠️ 需要清理重建 |
| Zookeeper 数据混乱 | ⚠️ 需要清理重建 |

---

## 一键修复脚本

将以下内容保存为 `fix_kafka_full.sh`，上传到 master 节点执行：

```bash
#!/bin/bash
# Kafka 3-Node Cluster Full Fix Script
# Run on master node

set -e

KAFKA_HOME="/opt/module/kafka"
NODES=("master" "slave1" "slave2")

echo "============================================================"
echo "  Kafka 3-Node Cluster Full Fix"
echo "============================================================"

# Function to execute command on remote node
exec_remote() {
    local node=$1
    shift
    ssh -o StrictHostKeyChecking=no root@$node "$@"
}

# Step 1: Stop all services
echo ""
echo "[Step 1] Stopping all Kafka and Zookeeper..."
for node in "${NODES[@]}"; do
    echo "  [$node] Stopping services..."
    exec_remote $node "cd $KAFKA_HOME && bin/kafka-server-stop.sh 2>/dev/null; pkill -9 -f kafka.Kafka 2>/dev/null; sleep 2"
    exec_remote $node "cd $KAFKA_HOME && bin/zookeeper-server-stop.sh 2>/dev/null; pkill -9 -f QuorumPeerMain 2>/dev/null; sleep 2"
done

sleep 5

# Step 2: Clean Kafka data directories
echo ""
echo "[Step 2] Cleaning Kafka data directories..."
for node in "${NODES[@]}"; do
    echo "  [$node] Cleaning data..."
    exec_remote $node "mv $KAFKA_HOME/data $KAFKA_HOME/data.bak.\$(date +%Y%m%d%H%M%S) 2>/dev/null; mkdir -p $KAFKA_HOME/data"
    exec_remote $node "rm -rf /tmp/kafka-logs/* 2>/dev/null"
done

# Step 3: Clean and init Zookeeper data
echo ""
echo "[Step 3] Cleaning Zookeeper data..."
declare -A BROKER_IDS=(["master"]=0 ["slave1"]=1 ["slave2"]=2)
for node in "${NODES[@]}"; do
    echo "  [$node] Init ZK data (broker.id=${BROKER_IDS[$node]})..."
    exec_remote $node "rm -rf /tmp/zookeeper/* 2>/dev/null; mkdir -p /tmp/zookeeper"
    exec_remote $node "echo ${BROKER_IDS[$node]} > /tmp/zookeeper/myid"
done

# Step 4: Start Zookeeper
echo ""
echo "[Step 4] Starting Zookeeper on all nodes..."
for node in "${NODES[@]}"; do
    echo "  [$node] Starting ZK..."
    exec_remote $node "cd $KAFKA_HOME && nohup bin/zookeeper-server-start.sh config/zookeeper.properties > zk.log 2>&1 &"
done

echo "  Waiting 15s for ZK to start..."
sleep 15

# Verify ZK
for node in "${NODES[@]}"; do
    status=$(exec_remote $node "jps | grep QuorumPeerMain" && echo "OK" || echo "FAIL")
    echo "    [$node] ZK: $status"
done

# Step 5: Start Kafka
echo ""
echo "[Step 5] Starting Kafka on all nodes..."
for node in "${NODES[@]}"; do
    echo "  [$node] Starting Kafka..."
    exec_remote $node "cd $KAFKA_HOME && nohup bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &"
    sleep 10
done

echo "  Waiting 20s for Kafka to stabilize..."
sleep 20

# Step 6: Verify cluster
echo ""
echo "[Step 6] Verifying cluster..."
echo ""
echo "  Zookeeper brokers:"
exec_remote master "$KAFKA_HOME/bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids 2>/dev/null | grep -E '\[.*\]'"

echo ""
echo "  Broker connectivity:"
for node in "${NODES[@]}"; do
    result=$(exec_remote master "timeout 5 $KAFKA_HOME/bin/kafka-broker-api-versions.sh --bootstrap-server $node:9092 2>&1 | head -1")
    if echo "$result" | grep -q "broker.id\|ApiVersions"; then
        echo "    [$node]: OK"
    else
        echo "    [$node]: FAIL - $result"
    fi
done

# Step 7: Create topics
echo ""
echo "[Step 7] Creating Kafka topics..."
TOPICS=("device_state:3" "sensor_metrics:3" "highfreq_sensor:3" "sensor_raw:3" "log_raw:3" "ChangeRecord:3" "Feature_log:3" "alert_topic:3" "device_metrics:3")
for entry in "${TOPICS[@]}"; do
    topic="${entry%%:*}"
    partitions="${entry##*:}"
    result=$(exec_remote master "$KAFKA_HOME/bin/kafka-topics.sh --create --topic $topic --partitions $partitions --replication-factor 1 --bootstrap-server master:9092 2>&1")
    if echo "$result" | grep -q "Created\|already exists"; then
        echo "  $topic: OK"
    else
        echo "  $topic: $result"
    fi
done

echo ""
echo "  Topic list:"
exec_remote master "$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server master:9092" | while read line; do
    echo "    $line"
done

echo ""
echo "============================================================"
echo "  DONE! Cluster is ready."
echo "============================================================"
echo ""
echo "Next steps:"
echo "  1. Start Flume:"
echo "     cd /opt/module/flume"
echo "     bin/flume-ng agent -n a1 -c conf -f conf/MyFlume.conf"
echo ""
echo "  2. Start data generation:"
echo "     cd /data_log"
echo "     bash generate_change_data.sh &"
echo ""
echo "  3. Submit Flink job:"
echo "     cd /opt/module/flink"
echo "     bin/flink run -d /opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
```

---

## 手动执行步骤

如果你不想用脚本，按以下步骤手动执行：

### 1. 在所有节点停止服务

**在 master、slave1、slave2 上都执行：**
```bash
cd /opt/module/kafka
bin/kafka-server-stop.sh 2>/dev/null
bin/zookeeper-server-stop.sh 2>/dev/null
pkill -9 -f kafka.Kafka 2>/dev/null
pkill -9 -f QuorumPeerMain 2>/dev/null
```

### 2. 清理 Kafka 数据目录

**在所有节点执行：**
```bash
# 备份并重建数据目录
mv /opt/module/kafka/data /opt/module/kafka/data.bak.$(date +%Y%m%d%H%M%S)
mkdir -p /opt/module/kafka/data

# 清理临时日志
rm -rf /tmp/kafka-logs/*
```

### 3. 清理并初始化 Zookeeper 数据

**在 master 执行：**
```bash
rm -rf /tmp/zookeeper/*
mkdir -p /tmp/zookeeper
echo 0 > /tmp/zookeeper/myid
```

**在 slave1 执行：**
```bash
rm -rf /tmp/zookeeper/*
mkdir -p /tmp/zookeeper
echo 1 > /tmp/zookeeper/myid
```

**在 slave2 执行：**
```bash
rm -rf /tmp/zookeeper/*
mkdir -p /tmp/zookeeper
echo 2 > /tmp/zookeeper/myid
```

### 4. 启动 Zookeeper

**在所有节点执行：**
```bash
cd /opt/module/kafka
nohup bin/zookeeper-server-start.sh config/zookeeper.properties > zk.log 2>&1 &
```

等待 15 秒，验证：
```bash
jps | grep QuorumPeerMain
```

### 5. 启动 Kafka

**在 master 执行：**
```bash
cd /opt/module/kafka
nohup bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &
```

等待 10 秒，然后在 slave1 执行：
```bash
cd /opt/module/kafka
nohup bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &
```

再等待 10 秒，在 slave2 执行：
```bash
cd /opt/module/kafka
nohup bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &
```

等待 20 秒后验证：
```bash
jps | grep Kafka
```

### 6. 验证集群

在 master 上执行：
```bash
# 检查 Zookeeper 中的 broker 列表
/opt/module/kafka/bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids

# 检查 broker 连通性
/opt/module/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server master:9092
/opt/module/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server slave1:9092
/opt/module/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server slave2:9092
```

### 7. 创建 Topics

```bash
for topic in device_state sensor_metrics highfreq_sensor sensor_raw log_raw ChangeRecord Feature_log alert_topic device_metrics; do
  /opt/module/kafka/bin/kafka-topics.sh --create --topic $topic --partitions 3 --replication-factor 1 --bootstrap-server master:9092
done
```

验证：
```bash
/opt/module/kafka/bin/kafka-topics.sh --list --bootstrap-server master:9092
```

---

## 问题总结

| 问题 | 原因 | 解决方案 |
|------|------|---------|
| /etc/hosts 不完整 | 节点间无法通过主机名通信 | 添加所有节点的 IP-主机名映射 |
| Kafka listeners 配置错误 | Broker 间无法建立连接 | 配置 `listeners=0.0.0.0:9092` 和 `advertised.listeners=主机名:9092` |
| Kafka 数据目录损坏 | `AtomicMoveNotSupportedException` 导致 log dir 失败 | 清理并重建数据目录 |
| Zookeeper 数据混乱 | myid 文件可能缺失或错误 | 重新创建 myid 文件并清理 ZK 数据 |

---

## 后续步骤

修复完成后，按顺序启动：

1. **启动 Flume**：采集数据到 Kafka
2. **启动数据生成脚本**：产生模拟数据
3. **提交 Flink 作业**：消费 Kafka 数据并进行特征计算

```bash
# 1. Flume
cd /opt/module/flume
bin/flume-ng agent -n a1 -c conf -f conf/MyFlume.conf -Dflume.root.logger=INFO,console

# 2. 数据生成
cd /data_log
bash generate_change_data.sh &
bash generate_highfreq_sensor.sh &

# 3. Flink
cd /opt/module/flink
bin/flink run -d /opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar
```
