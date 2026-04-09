import paramiko
import time

servers = [
    {"ip": "192.168.45.100", "hostname": "master"},
    {"ip": "192.168.45.101", "hostname": "slave1"},
    {"ip": "192.168.45.102", "hostname": "slave2"},
]
password = "123456"
username = "root"

def run_cmd(ssh, cmd):
    stdin, stdout, stderr = ssh.exec_command(cmd)
    return stdout.read().decode('utf-8', errors='replace').strip(), stderr.read().decode('utf-8', errors='replace').strip()

# 1. Kill processes
for s in servers:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(s["ip"], username=username, password=password, timeout=10)
        print(f"[{s['hostname']}] Killing Kafka and Zookeeper processes...")
        run_cmd(ssh, "pkill -f Kafka")
        run_cmd(ssh, "pkill -f QuorumPeerMain")
        run_cmd(ssh, "pkill -f ZooKeeperServerMain")
    except Exception as e:
        print(f"[{s['hostname']}] Error: {e}")
    finally:
        ssh.close()

time.sleep(3)

# 2. Clear Data and Start Zookeeper
for s in servers:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(s["ip"], username=username, password=password, timeout=10)
        print(f"[{s['hostname']}] Clearing old data and starting Zookeeper...")
        # Clear ZK data except myid
        run_cmd(ssh, "rm -rf /opt/module/zookeeper/zkData/version-2")
        run_cmd(ssh, "rm -rf /opt/module/zookeeper/logs/*")
        
        # Clear Kafka data
        run_cmd(ssh, "rm -rf /opt/module/kafka/data/*")
        run_cmd(ssh, "rm -rf /opt/module/kafka/logs/*")
        
        # Start Zookeeper
        out, err = run_cmd(ssh, "source /etc/profile; /opt/module/zookeeper/bin/zkServer.sh start")
        print(f"[{s['hostname']}] ZK Start: {out} | {err}")
    except Exception as e:
        print(f"[{s['hostname']}] Error: {e}")
    finally:
        ssh.close()

time.sleep(5)

# 3. Check ZK status and Start Kafka
for s in servers:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(s["ip"], username=username, password=password, timeout=10)
        out, err = run_cmd(ssh, "source /etc/profile; /opt/module/zookeeper/bin/zkServer.sh status")
        print(f"[{s['hostname']}] ZK Status: {out}")
        
        print(f"[{s['hostname']}] Starting Kafka...")
        out, err = run_cmd(ssh, "source /etc/profile; /opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties")
        print(f"[{s['hostname']}] Kafka Start: {out} | {err}")
    except Exception as e:
        print(f"[{s['hostname']}] Error: {e}")
    finally:
        ssh.close()

time.sleep(5)

# 4. Check Kafka processes
for s in servers:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(s["ip"], username=username, password=password, timeout=10)
        out, err = run_cmd(ssh, "source /etc/profile; jps | grep Kafka")
        print(f"[{s['hostname']}] Kafka Process: {out}")
    except Exception as e:
        print(f"[{s['hostname']}] Error: {e}")
    finally:
        ssh.close()
