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
        
        # Get parent process of 58781
        out, err = run_cmd(ssh, "ps -ef | grep 2181 | grep -v grep")
        print(f"[{s['hostname']}] Processes with 2181:\n{out}")
        
        out, err = run_cmd(ssh, "netstat -tlnp | grep 2181")
        pid = out.split()[-1].split('/')[0] if out else ""
        if pid:
            out2, err2 = run_cmd(ssh, f"ps -f -p {pid}")
            print(f"[{s['hostname']}] Process {pid}:\n{out2}")
        
    except Exception as e:
        print(f"[{s['hostname']}] Error: {e}")
    finally:
        ssh.close()
