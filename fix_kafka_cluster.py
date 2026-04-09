#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka  cluster diagnosis and fix script
Diagnose:
  1. Check /etc/hosts on all 3 nodes
  2. Check Kafka listening ports
  3. Check Kafka broker registration in Zookeeper
  4. Fix config and restart Kafka
  5. Verify cluster status
"""

import subprocess
import time
import sys

NODES = {
    "master": "192.168.45.100",
    "slave1": "192.168.45.101",
    "slave2": "192.168.45.102"
}
PASSWORD = "123456"
SSH_USER = "root"

def ssh_cmd(node, cmd, timeout=15):
    ip = NODES[node]
    full_cmd = 'sshpass -p %s ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 %s@%s "%s"' % (
        PASSWORD, SSH_USER, ip, cmd)
    try:
        result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except subprocess.TimeoutExpired:
        return -1, "", "Timeout"
    except Exception as e:
        return -1, "", str(e)

def ssh_cmd_raw(node, cmd, timeout=15):
    ip = NODES[node]
    full_cmd = 'sshpass -p %s ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 %s@%s %s' % (
        PASSWORD, SSH_USER, ip, cmd)
    try:
        result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except:
        return -1, "", "error"

def print_header(msg):
    print("\n" + "=" * 60)
    print("  " + msg)
    print("=" * 60)

def print_step(msg):
    print("\n>>> " + msg)

def diagnose_all():
    print_header("Step 1: Diagnose /etc/hosts")
    hosts_issues = []

    for node, ip in NODES.items():
        code, out, err = ssh_cmd(node, "cat /etc/hosts")
        print("\n[%s] /etc/hosts:" % node)
        if out:
            for line in out.split('\n'):
                print("  " + line)
            for check_node, check_ip in NODES.items():
                if check_ip not in out:
                    hosts_issues.append((node, check_node, check_ip))
                    print("  [!] Missing: %s (%s)" % (check_node, check_ip))
        else:
            print("  [!] Cannot read: " + err)

    print_header("Step 2: Diagnose Kafka process and port")
    kafka_status = {}

    for node in NODES:
        code, out, _ = ssh_cmd(node, "jps | grep Kafka")
        kafka_status[node] = "running" if (code == 0 and "Kafka" in out) else "not_running"
        status_str = "[OK]" if kafka_status[node] == "running" else "[NOT RUNNING]"
        print("[%s] Kafka process: %s" % (node, status_str))

        code, out, _ = ssh_cmd(node, "netstat -tlnp 2>/dev/null | grep 9092 || ss -tlnp | grep 9092")
        if out and '9092' in out:
            print("  Port 9092 listening: " + out.split('\n')[0])
        else:
            print("  Port 9092: [NOT LISTENING]")

    print_header("Step 3: Diagnose Kafka server.properties")
    listener_issues = []

    for node in NODES:
        print("\n[%s] key configs:" % node)
        code, listeners, _ = ssh_cmd(node,
            "grep -E '^listeners|^advertised.listeners|^broker.id|^zookeeper.connect' "
            "/opt/module/kafka/config/server.properties 2>/dev/null")
        if listeners:
            for line in listeners.split('\n'):
                if line.strip():
                    print("  " + line.strip())
                    if 'advertised.listeners' in line and '0.0.0.0' in line:
                        listener_issues.append((node, "advertised.listeners cannot use 0.0.0.0"))
        else:
            print("  [!] Cannot read config")

    print_header("Step 4: Zookeeper broker registration")
    code, out, _ = ssh_cmd("master",
        "/opt/module/kafka/bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids 2>/dev/null")
    print("Zookeeper /brokers/ids: " + (out if out else "[empty or error]"))

    return hosts_issues, listener_issues, kafka_status

def fix_hosts():
    print_header("Fix /etc/hosts")

    hosts_entries = "\n".join(["%s  %s" % (ip, name) for name, ip in NODES.items()])

    for node in NODES:
        print("\nFix [%s] /etc/hosts..." % node)

        code, current, _ = ssh_cmd(node, "cat /etc/hosts")
        current_lines = current.split('\n') if current else []

        new_lines = []
        for line in current_lines:
            # Skip old cluster entries
            if any(ip in line and not line.strip().startswith('#') for ip in NODES.values()):
                continue
            new_lines.append(line)

        new_content = '\n'.join(new_lines).rstrip() + '\n' + hosts_entries + '\n'

        # Backup
        ssh_cmd(node, "cp /etc/hosts /etc/hosts.bak.$(date +%%Y%%m%%d%%H%%M%%S)")
        print("  Backup done")

        # Write - use printf to avoid quote issues
        escaped = new_content.replace("'", "'\"'\"'")
        code, out, err = ssh_cmd_raw(node,
            "printf '%s' '%s' > /etc/hosts" % ('', escaped))

        if code != 0 and err:
            print("  [!] Write error: " + err[:100])
        else:
            print("  [OK] Updated /etc/hosts")

        # Verify
        code, verify, _ = ssh_cmd(node, "grep '192.168.45' /etc/hosts")
        if all(ip in verify for ip in NODES.values()):
            print("  [OK] Verified: all IPs present")
        else:
            print("  [!] Verification failed")
            print("  Content: " + verify[:200])

def fix_kafka_config():
    print_header("Fix Kafka listener config")

    for node, ip in NODES.items():
        print("\nCheck [%s] (%s)..." % (node, ip))

        # Backup first
        ssh_cmd(node,
            "cp /opt/module/kafka/config/server.properties "
            "/opt/module/kafka/config/server.properties.bak.$(date +%%Y%%m%%d%%H%%M%%S)")

        # Fix listeners
        new_listeners = "listeners=PLAINTEXT://0.0.0.0:9092"
        new_advertised = "advertised.listeners=PLAINTEXT://%s:9092" % node

        # Use sed to replace
        # Remove existing listeners line and add new one
        ssh_cmd(node,
            "sed -i '/^listeners=/d' /opt/module/kafka/config/server.properties")
        ssh_cmd(node,
            "sed -i '/^advertised.listeners=/d' /opt/module/kafka/config/server.properties")

        # Append new configs at end
        ssh_cmd(node,
            "echo '%s' >> /opt/module/kafka/config/server.properties" % new_listeners)
        ssh_cmd(node,
            "echo '%s' >> /opt/module/kafka/config/server.properties" % new_advertised)

        # Verify
        code, verify, _ = ssh_cmd(node,
            "grep -E '^listeners|^advertised.listeners' "
            "/opt/module/kafka/config/server.properties")
        print("  New config: " + (verify.replace('\n', ' | ') if verify else "[empty]"))

def restart_kafka(node):
    print("\nRestart [%s] Kafka..." % node)

    # Stop
    ssh_cmd(node, "cd /opt/module/kafka && bin/kafka-server-stop.sh 2>/dev/null; sleep 3")
    print("  Stop command sent")

    # Start
    code, _, err = ssh_cmd(node,
        "cd /opt/module/kafka && nohup bin/kafka-server-start.sh "
        "config/server.properties > kafka.log 2>&1 &")
    if code == 0:
        print("  Start command sent")
    else:
        print("  [!] Error: " + err[:100])

    print("  Waiting 10s for startup...")
    time.sleep(10)

    # Verify
    code, out, _ = ssh_cmd(node, "jps | grep Kafka")
    if "Kafka" in out:
        print("  [OK] Kafka process running")

        # Check port
        code2, port_out, _ = ssh_cmd(node,
            "netstat -tlnp 2>/dev/null | grep 9092 || ss -tlnp | grep 9092")
        if port_out and '9092' in port_out:
            print("  [OK] Port 9092 listening")
        else:
            print("  [!] Port 9092 not listening yet, check logs:")
            code3, log, _ = ssh_cmd(node,
                "tail -5 /opt/module/kafka/logs/server.log 2>/dev/null")
            print("  " + (log[:200] if log else "[no log]"))
    else:
        print("  [!] Kafka not running, check log:")
        code, log, _ = ssh_cmd(node, "tail -15 /opt/module/kafka/kafka.log 2>/dev/null")
        print("  " + (log[:300] if log else "[no log]"))

def verify_cluster():
    print_header("Verify Kafka cluster")

    time.sleep(5)

    # Test cluster
    print("\nTesting broker connectivity from master:")
    for node, ip in NODES.items():
        broker_str = "%s:9092" % node
        code, out, err = ssh_cmd("master",
            "/opt/module/kafka/bin/kafka-broker-api-versions.sh "
            "--bootstrap-server %s 2>&1 | head -2" % broker_str)
        if code == 0 and out:
            print("  [%s] %s: [OK]" % (node, broker_str))
        else:
            err_short = err[:80] if err else "no output"
            print("  [%s] %s: [FAIL] %s" % (node, broker_str, err_short))

    # Zookeeper
    print("\nZookeeper broker registration:")
    code, out, _ = ssh_cmd("master",
        "/opt/module/kafka/bin/zookeeper-shell.sh localhost:2181 "
        "ls /brokers/ids 2>/dev/null")
    print("  /brokers/ids: " + (out if out else "[empty]"))

def main():
    print("=" * 60)
    print("  Kafka Cluster Diagnosis & Fix Tool")
    print("=" * 60)
    print("  Master: %s" % NODES["master"])
    print("  Slave1: %s" % NODES["slave1"])
    print("  Slave2: %s" % NODES["slave2"])
    print("=" * 60)

    # Step 1: Diagnose
    hosts_issues, listener_issues, kafka_status = diagnose_all()

    # Check if fix is needed
    need_fix = (len(hosts_issues) > 0 or len(listener_issues) > 0 or
                any(s != "running" for s in kafka_status.values()))

    if not need_fix:
        print("\n*** Cluster looks OK, doing final verification ***")
        verify_cluster()
        return

    print("\n" + "=" * 60)
    print("  Issues found, starting fix...")
    print("=" * 60)
    print("  Hosts issues: %s" % hosts_issues)
    print("  Listener issues: %s" % listener_issues)
    print("  Kafka status: %s" % kafka_status)

    # Step 2: Fix hosts
    fix_hosts()

    # Step 3: Fix Kafka config
    fix_kafka_config()

    # Step 4: Restart Kafka on all nodes
    print_header("Restart Kafka on all nodes")
    for node in NODES:
        restart_kafka(node)

    # Step 5: Verify
    verify_cluster()

    print("\n" + "=" * 60)
    print("  Fix complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Rebuild Kafka topics (multi-partition)")
    print("  2. Restart Flume")
    print("  3. Test Flink")

if __name__ == "__main__":
    main()
