#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kafka 3-Node Cluster Auto-Fix Script
Uses paramiko for SSH connections
"""

import paramiko
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
NODES = {
    "master": {"ip": "192.168.45.100", "broker_id": "0"},
    "slave1": {"ip": "192.168.45.101", "broker_id": "1"},
    "slave2": {"ip": "192.168.45.102", "broker_id": "2"}
}
USERNAME = "root"
PASSWORD = "123456"
KAFKA_HOME = "/opt/module/kafka"

class KafkaFixer:
    def __init__(self):
        self.clients = {}
        self.results = {}
        
    def connect(self, node_name, node_info):
        """Connect to a node via SSH"""
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=node_info["ip"],
                username=USERNAME,
                password=PASSWORD,
                timeout=10,
                banner_timeout=10,
                auth_timeout=10
            )
            self.clients[node_name] = client
            print(f"[OK] Connected to {node_name} ({node_info['ip']})")
            return True
        except Exception as e:
            print(f"[FAIL] Cannot connect to {node_name}: {e}")
            return False
    
    def exec_cmd(self, node, cmd, timeout=30):
        """Execute command on a node"""
        try:
            client = self.clients.get(node)
            if not client:
                return -1, "", "Not connected"
            stdin, stdout, stderr = client.exec_command(cmd, timeout=timeout)
            exit_code = stdout.channel.recv_exit_status()
            out = stdout.read().decode('utf-8', errors='ignore').strip()
            err = stderr.read().decode('utf-8', errors='ignore').strip()
            return exit_code, out, err
        except Exception as e:
            return -1, "", str(e)
    
    def fix_hosts(self, node):
        """Fix /etc/hosts on a node"""
        print(f"\n[{node}] Fixing /etc/hosts...")
        
        # Backup
        self.exec_cmd(node, "cp /etc/hosts /etc/hosts.bak.$(date +%Y%m%d%H%M%S)")
        
        # Create new hosts content
        hosts_content = """127.0.0.1   localhost
::1         localhost ip6-localhost ip6-loopback
fe00::0     ip6-localnet
ff00::0     ip6-mcastprefix
ff02::1     ip6-allnodes
ff02::2     ip6-allrouters
192.168.45.100  master
192.168.45.101  slave1
192.168.45.102  slave2
"""
        # Write using printf to avoid quoting issues
        cmd = """cat > /etc/hosts << 'EOFHOST'
127.0.0.1   localhost
::1         localhost ip6-localhost ip6-loopback
fe00::0     ip6-localnet
ff00::0     ip6-mcastprefix
ff02::1     ip6-allnodes
ff02::2     ip6-allrouters
192.168.45.100  master
192.168.45.101  slave1
192.168.45.102  slave2
EOFHOST"""
        
        exit_code, out, err = self.exec_cmd(node, cmd)
        
        # Verify
        exit_code2, verify, _ = self.exec_cmd(node, "grep '192.168.45' /etc/hosts")
        if exit_code2 == 0 and all(ip in verify for ip in ["192.168.45.100", "192.168.45.101", "192.168.45.102"]):
            print(f"  [OK] /etc/hosts fixed")
            return True
        else:
            print(f"  [FAIL] /etc/hosts verification failed")
            print(f"  Content: {verify[:200]}")
            return False
    
    def fix_kafka_config(self, node):
        """Fix Kafka listener configuration"""
        print(f"\n[{node}] Fixing Kafka config...")
        
        # Backup
        self.exec_cmd(node, f"cp {KAFKA_HOME}/config/server.properties {KAFKA_HOME}/config/server.properties.bak.$(date +%Y%m%d%H%M%S)")
        
        # Remove old listeners config
        self.exec_cmd(node, f"sed -i '/^listeners=/d' {KAFKA_HOME}/config/server.properties")
        self.exec_cmd(node, f"sed -i '/^advertised.listeners=/d' {KAFKA_HOME}/config/server.properties")
        
        # Add correct config
        self.exec_cmd(node, f"echo 'listeners=PLAINTEXT://0.0.0.0:9092' >> {KAFKA_HOME}/config/server.properties")
        self.exec_cmd(node, f"echo 'advertised.listeners=PLAINTEXT://{node}:9092' >> {KAFKA_HOME}/config/server.properties")
        
        # Verify
        exit_code, verify, _ = self.exec_cmd(node, f"grep -E '^listeners|^advertised.listeners' {KAFKA_HOME}/config/server.properties")
        if exit_code == 0 and "0.0.0.0:9092" in verify and f"{node}:9092" in verify:
            print(f"  [OK] Config fixed")
            print(f"  {verify}")
            return True
        else:
            print(f"  [WARN] Config may not be correct: {verify}")
            return False
    
    def stop_kafka(self, node):
        """Stop Kafka on a node"""
        print(f"\n[{node}] Stopping Kafka...")
        self.exec_cmd(node, f"cd {KAFKA_HOME} && bin/kafka-server-stop.sh 2>/dev/null; sleep 3")
        
        # Verify stopped
        exit_code, out, _ = self.exec_cmd(node, "jps | grep Kafka")
        if "Kafka" not in out:
            print(f"  [OK] Kafka stopped")
            return True
        else:
            print(f"  [WARN] Kafka still running, force kill...")
            self.exec_cmd(node, "pkill -9 -f kafka.Kafka")
            return True
    
    def start_kafka(self, node):
        """Start Kafka on a node"""
        print(f"\n[{node}] Starting Kafka...")
        self.exec_cmd(node, f"cd {KAFKA_HOME} && nohup bin/kafka-server-start.sh config/server.properties > kafka.log 2>&1 &")
        
        # Wait for startup
        print(f"  Waiting 15s for startup...")
        time.sleep(15)
        
        # Verify
        exit_code, out, _ = self.exec_cmd(node, "jps | grep Kafka")
        if "Kafka" in out:
            print(f"  [OK] Kafka process running")
            
            # Check port
            exit_code2, port_out, _ = self.exec_cmd(node, "netstat -tlnp 2>/dev/null | grep 9092 || ss -tlnp | grep 9092")
            if port_out and "9092" in port_out:
                print(f"  [OK] Port 9092 listening")
                return True
            else:
                print(f"  [WARN] Port 9092 not listening yet")
                # Check log
                exit_code3, log, _ = self.exec_cmd(node, f"tail -5 {KAFKA_HOME}/logs/server.log 2>/dev/null || tail -5 {KAFKA_HOME}/kafka.log 2>/dev/null")
                print(f"  Log: {log[:300]}")
                return False
        else:
            print(f"  [FAIL] Kafka not started")
            exit_code, log, _ = self.exec_cmd(node, f"tail -10 {KAFKA_HOME}/kafka.log 2>/dev/null")
            print(f"  Error log: {log[:500]}")
            return False
    
    def rebuild_topics(self):
        """Rebuild all Kafka topics"""
        print("\n" + "="*60)
        print("  Rebuilding Kafka topics (3 partitions)")
        print("="*60)
        
        topics = [
            "device_state", "sensor_metrics", "highfreq_sensor",
            "sensor_raw", "log_raw", "ChangeRecord",
            "Feature_log", "alert_topic", "device_metrics"
        ]
        
        # Delete old topics
        print("\n[Step 1] Deleting old topics...")
        for topic in topics:
            exit_code, out, err = self.exec_cmd("master", 
                f"{KAFKA_HOME}/bin/kafka-topics.sh --delete --topic {topic} --bootstrap-server master:9092 2>&1")
            print(f"  {topic}: {out[:100] if out else 'deleted or not exist'}")
        
        time.sleep(5)
        
        # Create new topics
        print("\n[Step 2] Creating new topics (3 partitions)...")
        for topic in topics:
            exit_code, out, err = self.exec_cmd("master",
                f"{KAFKA_HOME}/bin/kafka-topics.sh --create --topic {topic} --partitions 3 --replication-factor 1 --bootstrap-server master:9092 2>&1")
            if "Created" in out or "already exists" in out:
                print(f"  {topic}: [OK]")
            else:
                print(f"  {topic}: {out[:100]}")
        
        # Verify
        print("\n[Step 3] Verifying topics...")
        exit_code, out, _ = self.exec_cmd("master",
            f"{KAFKA_HOME}/bin/kafka-topics.sh --describe --bootstrap-server master:9092 2>&1")
        print(out)
    
    def verify_cluster(self):
        """Verify cluster status"""
        print("\n" + "="*60)
        print("  Final Verification")
        print("="*60)
        
        # Check Zookeeper
        print("\n[Zookeeper broker registration]")
        exit_code, out, _ = self.exec_cmd("master",
            f"{KAFKA_HOME}/bin/zookeeper-shell.sh localhost:2181 ls /brokers/ids 2>/dev/null")
        print(f"  /brokers/ids: {out}")
        
        # Check each broker
        print("\n[Broker connectivity from master]")
        for node in NODES:
            exit_code, out, err = self.exec_cmd("master",
                f"timeout 5 {KAFKA_HOME}/bin/kafka-broker-api-versions.sh --bootstrap-server {node}:9092 2>&1 | head -1")
            status = "[OK]" if (exit_code == 0 and ("broker.id" in out or "ApiVersions" in out)) else "[FAIL]"
            print(f"  [{node}]: {status} {out[:80]}")
        
        # Check topic leaders
        print("\n[Topic partition leaders]")
        exit_code, out, _ = self.exec_cmd("master",
            f"{KAFKA_HOME}/bin/kafka-topics.sh --describe --topic device_state --bootstrap-server master:9092 2>/dev/null")
        if out:
            for line in out.split('\n'):
                if 'Partition' in line or 'Leader' in line:
                    print(f"  {line}")
        
        return True
    
    def run(self):
        """Main execution flow"""
        print("="*60)
        print("  Kafka 3-Node Cluster Auto-Fix Tool")
        print("="*60)
        print(f"  Nodes: {', '.join(NODES.keys())}")
        print("="*60)
        
        # Step 1: Connect to all nodes
        print("\n[Step 1] Connecting to all nodes...")
        for node_name, node_info in NODES.items():
            self.connect(node_name, node_info)
        
        if len(self.clients) < 3:
            print("\n[ERROR] Could not connect to all nodes. Aborting.")
            return False
        
        # Step 2: Fix /etc/hosts on all nodes
        print("\n" + "="*60)
        print("  [Step 2] Fixing /etc/hosts")
        print("="*60)
        for node in NODES:
            self.fix_hosts(node)
        
        # Step 3: Fix Kafka config on all nodes
        print("\n" + "="*60)
        print("  [Step 3] Fixing Kafka listener config")
        print("="*60)
        for node in NODES:
            self.fix_kafka_config(node)
        
        # Step 4: Stop Kafka on all nodes
        print("\n" + "="*60)
        print("  [Step 4] Stopping Kafka on all nodes")
        print("="*60)
        for node in NODES:
            self.stop_kafka(node)
        
        time.sleep(3)
        
        # Step 5: Start Kafka on all nodes (with delay)
        print("\n" + "="*60)
        print("  [Step 5] Starting Kafka on all nodes")
        print("="*60)
        for node in NODES:
            self.start_kafka(node)
            time.sleep(5)  # Stagger startup
        
        # Step 6: Wait and verify cluster
        print("\n[Waiting 10s for cluster to stabilize...]")
        time.sleep(10)
        
        # Step 7: Rebuild topics
        self.rebuild_topics()
        
        # Step 8: Final verification
        self.verify_cluster()
        
        # Cleanup
        print("\n[Closing SSH connections...]")
        for client in self.clients.values():
            client.close()
        
        print("\n" + "="*60)
        print("  DONE!")
        print("="*60)
        print("\nNext steps:")
        print("  1. Restart Flume")
        print("  2. Start data generation scripts")
        print("  3. Restart Flink job")
        
        return True


if __name__ == "__main__":
    try:
        import paramiko
    except ImportError:
        print("Installing paramiko...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "paramiko"], check=True)
        import paramiko
    
    fixer = KafkaFixer()
    success = fixer.run()
    sys.exit(0 if success else 1)
