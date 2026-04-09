import paramiko

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

for s in servers:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(s["ip"], username=username, password=password, timeout=10)
        print(f"=== {s['hostname']} ({s['ip']}) ===")
        
        # Kill Kafka and ZK
        print("Killing processes...")
        run_cmd(ssh, "pkill -f Kafka")
        run_cmd(ssh, "pkill -f QuorumPeerMain")
        
        # Clear Kafka logs (to start fresh and avoid corrupted cluster ID)
        print("Clearing Kafka logs and ZK data to ensure clean start...")
        run_cmd(ssh, "rm -rf /opt/module/kafka/data/meta.properties")
        
        # Start proper ZK
        print("Starting ZK...")
        out, err = run_cmd(ssh, "/opt/module/zookeeper/bin/zkServer.sh start")
        print(out)
        
    except Exception as e:
        print(f"Failed to connect to {s['hostname']}: {e}")
    finally:
        ssh.close()
