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
        
        # Check QuorumPeerMain command line
        out, err = run_cmd(ssh, "ps -ef | grep QuorumPeerMain | grep -v grep")
        print(f"QuorumPeerMain:\n{out}")
        
        # Check Kafka command line
        out, err = run_cmd(ssh, "ps -ef | grep Kafka | grep -v grep")
        print(f"Kafka:\n{out}")
        
        # Check ZK cluster status
        out, err = run_cmd(ssh, "/opt/module/zookeeper/bin/zkServer.sh status")
        print(f"ZK status:\n{out}\n{err}")
        
    except Exception as e:
        print(f"Failed to connect to {s['hostname']}: {e}")
    finally:
        ssh.close()
