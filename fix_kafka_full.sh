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
exec_remote master "$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server master:9092"

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
