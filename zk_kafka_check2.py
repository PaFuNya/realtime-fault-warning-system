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
        
        # Check Zookeeper logs
        out, err = run_cmd(ssh, "cat /opt/module/zookeeper/logs/zookeeper-*.out | tail -n 20")
        if not out:
            out, err = run_cmd(ssh, "cat /opt/module/zookeeper/logs/zookeeper-root-server-*.out | tail -n 20")
        if not out:
            out, err = run_cmd(ssh, "cat /opt/module/zookeeper/bin/zookeeper.out | tail -n 20")
        print(f"ZK Logs:\n{out}")
        
        # Check Kafka logs
        out, err = run_cmd(ssh, "cat /opt/module/kafka/logs/server.log | tail -n 20")
        print(f"Kafka Logs:\n{out}")
        
        # Check myid
        out, err = run_cmd(ssh, "cat /opt/module/zookeeper/zkData/myid")
        print(f"myid in zkData:\n{out}")
        
        # Check zoo.cfg
        out, err = run_cmd(ssh, "cat /opt/module/zookeeper/conf/zoo.cfg | grep -v '^#' | grep -v '^$'")
        print(f"zoo.cfg:\n{out}")
        
        # Check server.properties
        out, err = run_cmd(ssh, "cat /opt/module/kafka/config/server.properties | grep -v '^#' | grep -v '^$' | grep 'log.dirs\|zookeeper.connect\|broker.id'")
        print(f"server.properties:\n{out}")
        
    except Exception as e:
        print(f"Failed to connect to {s['hostname']}: {e}")
    finally:
        ssh.close()
