#!/bin/bash
#============================================================
#  Kafka 3-Node Cluster Fix Script
#  Usage: upload to master, run: bash fix_kafka_cluster.sh
#============================================================

set -e

NODES=("master" "slave1" "slave2")
IPS=("192.168.45.100" "192.168.45.101" "192.168.45.102")

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err() { echo -e "${RED}[ERROR]${NC} $1"; }

echo "============================================================"
echo "  Kafka Cluster Fix Tool"
echo "============================================================"

#============================================================
# STEP 1: Fix /etc/hosts on all nodes
#============================================================
echo ""
echo "============================================================"
echo "  STEP 1: Fix /etc/hosts"
echo "============================================================"

for i in "${!NODES[@]}"; do
    node="${NODES[$i]}"
    ip="${IPS[$i]}"

    echo ""
    log "Processing [$node] ($ip)..."

    # SSH and fix hosts
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 $node << 'HEREDOC'
        HOSTS_LINE=""
        for n in master slave1 slave2; do
            case $n in
                master)  IP="192.168.45.100" ;;
                slave1) IP="192.168.45.101" ;;
                slave2) IP="192.168.45.102" ;;
            esac
            HOSTS_LINE="${HOSTS_LINE}${IP}  ${n}"$'\n'
        done

        # Backup
        cp /etc/hosts /etc/hosts.bak.$(date +%Y%m%d%H%M%S)

        # Remove old cluster entries (non-comment lines with 192.168.45)
        grep -v '^[^#]*192\.168\.45\.' /etc/hosts > /tmp/hosts.new
        echo "$HOSTS_LINE" >> /tmp/hosts.new
        mv /tmp/hosts.new /etc/hosts

        # Verify
        echo "--- /etc/hosts ---"
        cat /etc/hosts
HEREDOC

done

#============================================================
# STEP 2: Check and fix Kafka listener config on all nodes
#============================================================
echo ""
echo "============================================================"
echo "  STEP 2: Fix Kafka listener config"
echo "============================================================"

for i in "${!NODES[@]}"; do
    node="${NODES[$i]}"
    ip="${IPS[$i]}"

    echo ""
    log "Checking [$node] Kafka config..."

    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 $node << 'HEREDOC'
        CONF="/opt/module/kafka/config/server.properties"

        echo "--- Current config ---"
        grep -E '^listeners|^advertised.listeners|^broker.id' "$CONF" 2>/dev/null || echo "(none)"

        # Backup
        cp "$CONF" "${CONF}.bak.$(date +%Y%m%d%H%M%S)"

        # Fix listeners (bind to all interfaces)
        sed -i 's|^#*listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092|' "$CONF"
        if ! grep -q '^listeners=' "$CONF"; then
            echo "listeners=PLAINTEXT://0.0.0.0:9092" >> "$CONF"
        fi

        # Fix advertised.listeners (advertise by hostname, not 0.0.0.0)
        sed -i 's|^#*advertised.listeners=.*|advertised.listeners=PLAINTEXT://'"$(hostname)"':9092|' "$CONF"
        if ! grep -q '^advertised.listeners=' "$CONF"; then
            echo "advertised.listeners=PLAINTEXT://$(hostname):9092" >> "$CONF"
        fi

        echo "--- Updated config ---"
        grep -E '^listeners|^advertised.listeners|^broker.id' "$CONF"
HEREDOC

done

#============================================================
# STEP 3: Stop Kafka on all nodes
#============================================================
echo ""
echo "============================================================"
echo "  STEP 3: Stop Kafka on all nodes"
echo "============================================================"

for node in "${NODES[@]}"; do
    echo ""
    log "Stopping Kafka on [$node]..."
    ssh -o StrictHostKeyChecking=no $node "cd /opt/module/kafka && bin/kafka-server-stop.sh 2>/dev/null; sleep 3; jps | grep Kafka || echo 'Kafka stopped'" &
done
wait
log "All Kafka stopped"

#============================================================
# STEP 4: Start Kafka on all nodes (with delay)
#============================================================
echo ""
echo "============================================================"
echo "  STEP 4: Start Kafka on all nodes"
echo "============================================================"

for i in "${!NODES[@]}"; do
    node="${NODES[$i]}"

    echo ""
    log "Starting Kafka on [$node]..."
    ssh -o StrictHostKeyChecking=no $node "cd /opt/module/kafka && nohup bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &" &

    # Stagger startup by 5s to avoid ZK election contention
    sleep 5
done

wait
log "All start commands sent, waiting 15s for initialization..."
sleep 15

#============================================================
# STEP 5: Verify cluster
#============================================================
echo ""
echo "============================================================"
echo "  STEP 5: Verify cluster"
echo "============================================================"

echo ""
log "Checking Kafka process on all nodes:"
for node in "${NODES[@]}"; do
    ssh -o StrictHostKeyChecking=no $node "jps | grep Kafka" &
done
wait

echo ""
log "Checking port 9092 listening:"
for node in "${NODES[@]}"; do
    echo -n "  [$node]: "
    ssh -o StrictHostKeyChecking=no $node "netstat -tlnp 2>/dev/null | grep 9092 || ss -tlnp | grep 9092" 2>/dev/null | head -1 || echo "(not listening)"
done

echo ""
log "Zookeeper broker registration:"
/opt/module/kafka/bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids 2>/dev/null

echo ""
log "Broker connectivity test:"
for node in "${NODES[@]}"; do
    echo -n "  [$node]: "
    timeout 5 /opt/module/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server ${node}:9092 2>&1 | head -1 || echo "FAIL"
done

#============================================================
# STEP 6: Rebuild topics
#============================================================
echo ""
echo "============================================================"
echo "  STEP 6: Rebuild Kafka topics (3-partition)"
echo "============================================================"

TOPICS=(
    "device_state:3"
    "sensor_metrics:3"
    "highfreq_sensor:3"
    "sensor_raw:3"
    "log_raw:3"
    "ChangeRecord:3"
    "Feature_log:3"
    "alert_topic:3"
    "device_metrics:3"
)

for entry in "${TOPICS[@]}"; do
    topic="${entry%%:*}"
    partitions="${entry##*:}"

    echo ""
    log "Deleting topic [$topic]..."
    /opt/module/kafka/bin/kafka-topics.sh --delete --topic "$topic" --bootstrap-server master:9092 2>/dev/null || true
done

echo ""
log "Waiting 5s for topic deletion..."
sleep 5

for entry in "${TOPICS[@]}"; do
    topic="${entry%%:*}"
    partitions="${entry##*:}"

    echo ""
    log "Creating topic [$topic] with $partitions partitions..."
    /opt/module/kafka/bin/kafka-topics.sh --create \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --bootstrap-server master:9092 2>/dev/null || warn "Topic $topic may already exist"
done

echo ""
log "Verifying topics:"
/opt/module/kafka/bin/kafka-topics.sh --list --bootstrap-server master:9092

echo ""
log "Topic details:"
for entry in "${TOPICS[@]}"; do
    topic="${entry%%:*}"
    /opt/module/kafka/bin/kafka-topics.sh --describe --topic "$topic" --bootstrap-server master:9092 2>/dev/null
done

#============================================================
# SUMMARY
#============================================================
echo ""
echo "============================================================"
echo "  DONE!"
echo "============================================================"
echo ""
echo "Next steps:"
echo "  1. Restart Flume:"
echo "     cd /opt/module/flume && bin/flume-ng agent -n a1 -c conf -f conf/MyFlume.conf -Dflume.root.logger=INFO,console"
echo ""
echo "  2. Start data generation scripts (if needed):"
echo "     cd /data_log && bash generate_change_data.sh &"
echo ""
echo "  3. Restart Flink job:"
echo "     cd /opt/module/flink && bin/flink run -d /path/to/realtime-data-process-1.0-SNAPSHOT.jar"
echo ""
echo "  4. Test Kafka consumer:"
echo "     kafka-console-consumer.sh --topic device_state --bootstrap-server master:9092 --from-beginning --max-messages 3"
echo ""
