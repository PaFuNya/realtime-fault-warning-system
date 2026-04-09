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
        
        # Check running java processes properly
        out, err = run_cmd(ssh, "source /etc/profile; jps")
        print(f"JPS:\n{out}")
        
        # Check port usage
        out, err = run_cmd(ssh, "netstat -tlnp | grep -E '2181|2888|3888|9092'")
        print(f"Ports:\n{out}")
        
    except Exception as e:
        print(f"Failed to connect to {s['hostname']}: {e}")
    finally:
        ssh.close()
