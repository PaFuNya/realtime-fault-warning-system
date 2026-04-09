#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fix Kafka data directory corruption and restart cluster
"""

import paramiko
import time

NODES = {
    "master": {"ip": "192.168.45.100", "broker_id": "0"},
    "slave1": {"ip": "192.168.45.101", "broker_id": "1"},
    "slave2": {"ip": "192.168.45.102", "broker_id": "2"}
}
USERNAME = "root"
PASSWORD = "123456"
KAFKA_HOME = "/opt/module/kafka"

def connect(node_name, node_info):
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=node_info["ip"],
            username=USERNAME,
            password=PASSWORD,
            timeout=10
        )
        return client
    except Exception as e:
        print(f"[FAIL] Cannot connect to {node_name}: {e}")
        return None

def exec_cmd(client, cmd, timeout=30):
    try:
        stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
        exit_code = stdout.channel.recv_exit_status()
        out = stdout.read().decode('utf-8', errors='ignore').strip()
        err = stderr.read().decode('utf-8', errors='ignore').strip()
        return exit_code, out, err
    except Exception as e:
        return -1, "", str(e)

print("="*60)
print("  Fix Kafka Data Directory and Restart Cluster")
print("="*60)

# Connect to all nodes
clients = {}
for node_name, node_info in NODES.items():
    clients[node_name] = connect(node_name, node_info)

if len(clients) < 3:
    print("[ERROR] Could not connect to all nodes")
    exit(1)

# Step 1: Stop all Kafka and Zookeeper
print("\n[Step 1] Stopping all Kafka and Zookeeper...")
for node in NODES:
    client = clients[node]
    print(f"  [{node}] Stopping Kafka...")
    exec_cmd(client, f"cd {KAFKA_HOME} && bin/kafka-server-stop.sh 2>/dev/null; pkill -9 -f kafka.Kafka 2>/dev/null; sleep 2")
    print(f"  [{node}] Stopping Zookeeper...")
    exec_cmd(client, f"cd {KAFKA_HOME} && bin/zookeeper-server-stop.sh 2>/dev/null; pkill -9 -f QuorumPeerMain 2>/dev/null; sleep 2")

time.sleep(5)

# Step 2: Clean up Kafka data directories
print("\n[Step 2] Cleaning up Kafka data directories...")
for node in NODES:
    client = clients[node]
    print(f"\n  [{node}]:")
    
    # Check current data dir
    code, out, _ = exec_cmd(client, f"ls -la {KAFKA_HOME}/data/ 2>/dev/null | head -10")
    print(f"    Current data dir: {out[:200] if out else 'empty or not exist'}")
    
    # Backup and clean
    print(f"    Backing up old data...")
    exec_cmd(client, f"mv {KAFKA_HOME}/data {KAFKA_HOME}/data.bak.$(date +%Y%m%d%H%M%S) 2>/dev/null; mkdir -p {KAFKA_HOME}/data")
    
    # Also clean /tmp/kafka-logs if exists
    exec_cmd(client, "rm -rf /tmp/kafka-logs/* 2>/dev/null")
    
    # Verify
    code, out, _ = exec_cmd(client, f"ls -la {KAFKA_HOME}/data/")
    print(f"    New data dir: {out}")

# Step 3: Clean Zookeeper data
print("\n[Step 3] Cleaning Zookeeper data...")
for node in NODES:
    client = clients[node]
    print(f"  [{node}] Cleaning ZK data...")
    exec_cmd(client, f"rm -rf /tmp/zookeeper/* 2>/dev/null; mkdir -p /tmp/zookeeper")
    # Create myid file
    broker_id = NODES[node]["broker_id"]
    exec_cmd(client, f"echo {broker_id} > /tmp/zookeeper/myid")
    code, out, _ = exec_cmd(client, "cat /tmp/zookeeper/myid")
    print(f"    ZK myid: {out}")

# Step 4: Start Zookeeper on all nodes
print("\n[Step 4] Starting Zookeeper on all nodes...")
for node in NODES:
    client = clients[node]
    print(f"  [{node}] Starting ZK...")
    exec_cmd(client, f"cd {KAFKA_HOME} && nohup bin/zookeeper-server-start.sh config/zookeeper.properties > zk.log 2>&1 &")

time.sleep(10)

# Verify ZK
print("\n  Verifying Zookeeper...")
for node in NODES:
    client = clients[node]
    code, out, _ = exec_cmd(client, "jps | grep QuorumPeerMain")
    status = "[OK]" if "QuorumPeerMain" in out else "[FAIL]"
    print(f"    [{node}] {status}")

# Step 5: Start Kafka on all nodes
print("\n[Step 5] Starting Kafka on all nodes...")
for node in NODES:
    client = clients[node]
    print(f"\n  [{node}] Starting Kafka...")
    exec_cmd(client, f"cd {KAFKA_HOME} && nohup bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &")
    time.sleep(10)  # Stagger startup
    
    # Check if started
    code, out, _ = exec_cmd(client, "jps | grep Kafka")
    if "Kafka" in out:
        print(f"    [OK] Kafka process running")
    else:
        print(f"    [FAIL] Kafka not running")
        code, log, _ = exec_cmd(client, f"tail -10 {KAFKA_HOME}/kafka.log")
        print(f"    Log: {log[:300]}")

# Wait for full startup
print("\n[Waiting 20s for cluster to stabilize...]")
time.sleep(20)

# Step 6: Verify cluster
print("\n[Step 6] Verifying cluster...")

# Check ZK brokers
client = clients["master"]
code, out, _ = exec_cmd(client, f"{KAFKA_HOME}/bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids 2>/dev/null")
print(f"\n  ZK /brokers/ids: {out}")

# Check broker connectivity
print("\n  Broker connectivity:")
for node in NODES:
    code, out, err = exec_cmd(client, f"timeout 5 {KAFKA_HOME}/bin/kafka-broker-api-versions.sh --bootstrap-server {node}:9092 2>&1 | head -1")
    status = "[OK]" if (code == 0 and ("broker.id" in out or "ApiVersions" in out)) else "[FAIL]"
    print(f"    [{node}]: {status}")

# Step 7: Create topics
print("\n[Step 7] Creating Kafka topics...")
topics = [
    ("device_state", 3),
    ("sensor_metrics", 3),
    ("highfreq_sensor", 3),
    ("sensor_raw", 3),
    ("log_raw", 3),
    ("ChangeRecord", 3),
    ("Feature_log", 3),
    ("alert_topic", 3),
    ("device_metrics", 3)
]

for topic, partitions in topics:
    code, out, _ = exec_cmd(client, f"{KAFKA_HOME}/bin/kafka-topics.sh --create --topic {topic} --partitions {partitions} --replication-factor 1 --bootstrap-server master:9092 2>&1")
    if "Created" in out or "already exists" in out:
        print(f"  {topic}: [OK]")
    else:
        print(f"  {topic}: {out[:80]}")

# Verify topics
print("\n  Topic list:")
code, out, _ = exec_cmd(client, f"{KAFKA_HOME}/bin/kafka-topics.sh --list --bootstrap-server master:9092")
for line in out.split('\n'):
    if line.strip():
        print(f"    {line}")

# Cleanup
for client in clients.values():
    client.close()

print("\n" + "="*60)
print("  DONE! Cluster is ready.")
print("="*60)
print("\nNext steps:")
print("  1. Restart Flume")
print("  2. Start data generation scripts")
print("  3. Restart Flink job")
