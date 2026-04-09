#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Deep diagnosis for Kafka startup failure
"""

import paramiko
import time

NODES = {
    "master": {"ip": "192.168.45.100"},
    "slave1": {"ip": "192.168.45.101"},
    "slave2": {"ip": "192.168.45.102"}
}
USERNAME = "root"
PASSWORD = "123456"

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
print("  Kafka Deep Diagnosis")
print("="*60)

clients = {}
for node_name, node_info in NODES.items():
    clients[node_name] = connect(node_name, node_info)

# Check 1: Zookeeper status
print("\n[Check 1] Zookeeper status on master:")
client = clients.get("master")
if client:
    code, out, _ = exec_cmd(client, "jps | grep -i quorum")
    print(f"  Zookeeper process: {out if out else 'NOT RUNNING'}")
    
    # Check ZK connection
    code, out, _ = exec_cmd(client, "echo 'ruok' | nc localhost 2181")
    print(f"  ZK health check: {out if out else 'No response'}")

# Check 2: Port availability
print("\n[Check 2] Port usage on each node:")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    print(f"\n  [{node}]:")
    code, out, _ = exec_cmd(client, "netstat -tlnp 2>/dev/null | grep -E '9092|2181' || ss -tlnp | grep -E '9092|2181'")
    if out:
        for line in out.split('\n')[:5]:
            print(f"    {line}")
    else:
        print("    No Kafka/ZK ports found")

# Check 3: Kafka log errors
print("\n[Check 3] Recent Kafka log errors:")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    print(f"\n  [{node}]:")
    code, out, _ = exec_cmd(client, "tail -20 /opt/module/kafka/logs/server.log 2>/dev/null | grep -i 'error\|fatal\|exception' | tail -5")
    if out:
        for line in out.split('\n'):
            print(f"    {line[:120]}")
    else:
        print("    No errors found or log not exist")

# Check 4: Full server.properties
print("\n[Check 4] Full Kafka config (key settings):")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    print(f"\n  [{node}]:")
    code, out, _ = exec_cmd(client, "grep -v '^#' /opt/module/kafka/config/server.properties | grep -v '^$' | head -20")
    if out:
        for line in out.split('\n'):
            if any(k in line for k in ['broker.id', 'listeners', 'zookeeper', 'log.dirs', 'offsets']):
                print(f"    {line}")

# Check 5: Firewall/iptables
print("\n[Check 5] Firewall status:")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    print(f"\n  [{node}]:")
    code, out, _ = exec_cmd(client, "iptables -L -n 2>/dev/null | head -5 || echo 'iptables not available'")
    if out:
        print(f"    {out[:150]}")

# Check 6: Try to manually start and capture full log
print("\n[Check 6] Attempting to start Kafka and capture output...")
for node in ["master"]:  # Just test master first
    client = clients.get(node)
    if not client:
        continue
    
    print(f"\n  [{node}] Starting Kafka in foreground for 10s...")
    
    # Start and capture first 50 lines of output
    code, out, err = exec_cmd(client, 
        "cd /opt/module/kafka && timeout 10 bin/kafka-server-start.sh config/server.properties 2>&1 | head -50", 
        timeout=15)
    
    if out:
        print("  Output:")
        for line in out.split('\n')[:20]:
            print(f"    {line[:100]}")
    if err:
        print("  Errors:")
        for line in err.split('\n')[:10]:
            print(f"    {line[:100]}")
    break

# Cleanup
for client in clients.values():
    if client:
        client.close()

print("\n" + "="*60)
print("  Diagnosis Complete")
print("="*60)
