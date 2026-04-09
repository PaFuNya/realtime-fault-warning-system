#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnose Zookeeper startup failure
"""

import paramiko

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
print("  Zookeeper Startup Failure Diagnosis")
print("="*60)

clients = {}
for node, ip in NODES.items():
    clients[node] = connect(node, ip)

# Check 1: Is ZK process running?
print("\n[Check 1] Zookeeper process status:")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    code, out, _ = exec_cmd(client, "jps | grep -i quorum")
    status = "RUNNING" if out else "NOT RUNNING"
    print(f"  [{node}]: {status}")

# Check 2: Check ZK log for errors
print("\n[Check 2] Zookeeper log errors:")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    print(f"\n  [{node}]:")
    code, out, _ = exec_cmd(client, f"tail -30 {KAFKA_HOME}/zk.log 2>/dev/null | grep -i 'error\|fatal\|exception\|cannot\|unable\|refuse' | tail -10")
    if out:
        for line in out.split('\n')[:5]:
            print(f"    {line[:120]}")
    else:
        # Try to get full log
        code, out, _ = exec_cmd(client, f"tail -20 {KAFKA_HOME}/zk.log 2>/dev/null")
        if out:
            print(f"    Last 20 lines:")
            for line in out.split('\n')[-10:]:
                print(f"      {line[:100]}")
        else:
            print(f"    No log file or empty")

# Check 3: Check zookeeper.properties config
print("\n[Check 3] Zookeeper configuration:")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    print(f"\n  [{node}]:")
    code, out, _ = exec_cmd(client, f"grep -v '^#' {KAFKA_HOME}/config/zookeeper.properties | grep -v '^$'")
    if out:
        for line in out.split('\n'):
            if line.strip():
                print(f"    {line}")

# Check 4: Check myid file
print("\n[Check 4] Zookeeper myid file:")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    code, out, _ = exec_cmd(client, "cat /tmp/zookeeper/myid 2>/dev/null")
    print(f"  [{node}]: myid={out if out else 'NOT FOUND'}")

# Check 5: Check if port 2181 is in use
print("\n[Check 5] Port 2181 usage:")
for node in NODES:
    client = clients.get(node)
    if not client:
        continue
    code, out, _ = exec_cmd(client, "netstat -tlnp 2>/dev/null | grep 2181 || ss -tlnp | grep 2181")
    if out:
        print(f"  [{node}]: {out}")
    else:
        print(f"  [{node}]: Port 2181 not in use")

# Check 6: Try to start ZK in foreground and capture error
print("\n[Check 6] Attempting to start ZK in foreground (master only):")
client = clients.get("master")
if client:
    print("  Starting ZK with timeout 10s...")
    code, out, err = exec_cmd(client, f"cd {KAFKA_HOME} && timeout 10 bin/zookeeper-server-start.sh config/zookeeper.properties 2>&1 | tail -30", timeout=15)
    if out:
        print("  Output:")
        for line in out.split('\n')[-15:]:
            if 'ERROR' in line or 'FATAL' in line or 'Exception' in line or 'Unable' in line or 'Cannot' in line or 'refuse' in line:
                print(f"    >>> {line[:100]}")
            else:
                print(f"    {line[:100]}")
    if err:
        print("  Stderr:")
        for line in err.split('\n')[:5]:
            print(f"    {line[:100]}")

# Cleanup
for client in clients.values():
    if client:
        client.close()

print("\n" + "="*60)
print("  Diagnosis Complete")
print("="*60)
