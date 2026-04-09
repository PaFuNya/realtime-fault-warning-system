#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fix Zookeeper memory issue and restart
"""

import paramiko
import time

NODES = {
    "master": "192.168.45.100",
    "slave1": "192.168.45.101",
    "slave2": "192.168.45.102"
}
USERNAME = "root"
PASSWORD = "123456"
KAFKA_HOME = "/opt/module/kafka"

def connect(node, ip):
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=ip, username=USERNAME, password=PASSWORD, timeout=10)
        return client
    except Exception as e:
        print(f"[FAIL] Cannot connect to {node}: {e}")
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
print("  Fix Zookeeper Memory Issue")
print("="*60)

clients = {}
for node, ip in NODES.items():
    clients[node] = connect(node, ip)

# Check current memory
print("\n[Step 1] Checking system memory:")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    code, out, _ = exec_cmd(client, "free -h")
    print(f"\n  [{node}] Memory:")
    for line in out.split('\n')[:3]:
        print(f"    {line}")

# Check current Java processes eating memory
print("\n[Step 2] Checking Java processes:")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    code, out, _ = exec_cmd(client, "ps aux | grep java | grep -v grep")
    print(f"\n  [{node}] Java processes:")
    if out:
        for line in out.split('\n'):
            print(f"    {line[:100]}")
    else:
        print("    None")

# Kill any hanging Java processes on master
print("\n[Step 3] Killing all Java processes on master:")
client = clients.get("master")
if client:
    exec_cmd(client, "pkill -9 -f java; sleep 2")
    code, out, _ = exec_cmd(client, "jps")
    print(f"  Processes after kill: {out if out else 'None'}")

# Free up memory
print("\n[Step 4] Freeing up memory:")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    exec_cmd(client, "echo 3 > /proc/sys/vm/drop_caches 2>/dev/null; sync")
    print(f"  [{node}] Cache cleared")

# Check memory again
print("\n[Step 5] Memory after cleanup:")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    code, out, _ = exec_cmd(client, "free -h | grep Mem")
    print(f"  [{node}]: {out}")

# Start Zookeeper with limited memory on master
print("\n[Step 6] Starting Zookeeper with memory limits:")

# First, verify slave1 and slave2 ZK are running
for node in ["slave1", "slave2"]:
    client = clients.get(node)
    if not client:
        continue
    code, out, _ = exec_cmd(client, "jps | grep QuorumPeerMain")
    if "QuorumPeerMain" in out:
        print(f"  [{node}] ZK already running")
    else:
        print(f"  [{node}] Starting ZK...")
        exec_cmd(client, f"cd {KAFKA_HOME} && nohup bin/zookeeper-server-start.sh config/zookeeper.properties > zk.log 2>&1 &")
        time.sleep(5)

# Start master ZK with limited heap
client = clients.get("master")
if client:
    print(f"  [master] Starting ZK with 256m heap...")
    # Set JVM heap limit for ZK
    exec_cmd(client, f"export KAFKA_HEAP_OPTS='-Xmx256m -Xms128m' && cd {KAFKA_HOME} && nohup bin/zookeeper-server-start.sh config/zookeeper.properties > zk.log 2>&1 &")
    time.sleep(10)

# Verify ZK
print("\n[Step 7] Verifying Zookeeper:")
time.sleep(10)
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    code, out, _ = exec_cmd(client, "jps | grep QuorumPeerMain")
    status = "RUNNING" if "QuorumPeerMain" in out else "NOT RUNNING"
    print(f"  [{node}]: {status}")

# Check ZK connectivity
print("\n[Step 8] Testing ZK connectivity:")
client = clients.get("master")
if client:
    code, out, _ = exec_cmd(client, f"echo 'ruok' | nc localhost 2181")
    print(f"  ZK ruok: {out if out else 'No response'}")
    
    code, out, _ = exec_cmd(client, f"{KAFKA_HOME}/bin/zookeeper-shell.sh localhost:2181 ls / 2>/dev/null | head -5")
    print(f"  ZK root: {out[:100] if out else 'Empty'}")

# Start Kafka
print("\n[Step 9] Starting Kafka:")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    
    # Check if already running
    code, out, _ = exec_cmd(client, "jps | grep Kafka")
    if "Kafka" in out:
        print(f"  [{node}] Kafka already running")
        continue
    
    print(f"  [{node}] Starting Kafka with limited memory...")
    exec_cmd(client, f"export KAFKA_HEAP_OPTS='-Xmx512m -Xms256m' && cd {KAFKA_HOME} && nohup bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &")
    time.sleep(15)
    
    code, out, _ = exec_cmd(client, "jps | grep Kafka")
    if "Kafka" in out:
        print(f"    [{node}] Kafka started")
    else:
        print(f"    [{node}] Kafka failed to start")
        code, log, _ = exec_cmd(client, f"tail -5 {KAFKA_HOME}/kafka.log")
        print(f"    Log: {log[:200]}")

# Final verification
print("\n[Step 10] Final verification:")
time.sleep(10)
client = clients.get("master")
if client:
    code, out, _ = exec_cmd(client, f"{KAFKA_HOME}/bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids 2>/dev/null")
    print(f"  ZK /brokers/ids: {out}")
    
    for node in NODES:
        code, out, _ = exec_cmd(client, f"timeout 5 {KAFKA_HOME}/bin/kafka-broker-api-versions.sh --bootstrap-server {node}:9092 2>&1 | head -1")
        status = "OK" if (code == 0 and ("broker.id" in out or "ApiVersions" in out)) else "FAIL"
        print(f"  [{node}]: {status}")

# Cleanup
for client in clients.values():
    if client:
        client.close()

print("\n" + "="*60)
print("  Fix attempt complete!")
print("="*60)
