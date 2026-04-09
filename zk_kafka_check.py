import paramiko
import sys

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
        print(f"--- Connected to {s['hostname']} ({s['ip']}) ---")
        
        # Check jps
        out, err = run_cmd(ssh, "jps")
        print(f"JPS:\n{out}")
        
        # Find ZK config
        out, err = run_cmd(ssh, "find / -name zoo.cfg 2>/dev/null | grep -v docker")
        if out:
            print(f"ZK Configs:\n{out}")
            cfg = out.split('\n')[0]
            out2, err2 = run_cmd(ssh, f"cat {cfg} | grep dataDir")
            print(f"dataDir: {out2}")
            data_dir = out2.split('=')[1].strip()
            out3, err3 = run_cmd(ssh, f"cat {data_dir}/myid")
            print(f"myid: {out3}")
            out4, err4 = run_cmd(ssh, f"ls -la {data_dir}")
            print(f"dataDir contents:\n{out4}")
            
        # Find Kafka config
        out, err = run_cmd(ssh, "find / -name server.properties 2>/dev/null | grep kafka")
        if out:
            print(f"Kafka Configs:\n{out}")
            cfg = out.split('\n')[0]
            out2, err2 = run_cmd(ssh, f"cat {cfg} | grep log.dirs")
            print(f"log.dirs: {out2}")
            log_dirs = out2.split('=')[1].strip()
            out4, err4 = run_cmd(ssh, f"ls -la {log_dirs} | grep meta.properties")
            print(f"Kafka meta.properties:\n{out4}")
            out5, err5 = run_cmd(ssh, f"cat {log_dirs}/meta.properties")
            print(f"Kafka meta.properties content:\n{out5}")
            
    except Exception as e:
        print(f"Failed to connect to {s['hostname']}: {e}")
    finally:
        ssh.close()
