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

# 1. Kill processes reliably
for s in servers:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(s["ip"], username=username, password=password, timeout=10)
        print(f"[{s['hostname']}] Killing Kafka and Zookeeper processes...")
        run_cmd(ssh, "ps -ef | grep '[K]afka' | awk '{print $2}' | xargs -r kill -9")
        run_cmd(ssh, "ps -ef | grep '[Q]uorumPeerMain' | awk '{print $2}' | xargs -r kill -9")
        run_cmd(ssh, "ps -ef | grep '[Z]ooKeeperServerMain' | awk '{print $2}' | xargs -r kill -9")
    except Exception as e:
        print(f"[{s['hostname']}] Error: {e}")
    finally:
        ssh.close()

time.sleep(3)

# 2. Check if anything still uses 2181 or 9092
for s in servers:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(s["ip"], username=username, password=password, timeout=10)
        out, err = run_cmd(ssh, "netstat -tlnp | grep -E '2181|9092'")
        print(f"[{s['hostname']}] Ports after kill:\n{out}")
    except Exception as e:
        print(f"[{s['hostname']}] Error: {e}")
    finally:
        ssh.close()
