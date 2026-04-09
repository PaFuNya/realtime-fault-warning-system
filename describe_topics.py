import paramiko

server = {"ip": "192.168.45.100", "hostname": "master"}
password = "123456"
username = "root"

topics = [
    "device_state",
    "sensor_metrics",
    "highfreq_sensor",
    "ChangeRecord",
    "sensor_raw",
    "log_raw"
]

def run_cmd(ssh, cmd):
    stdin, stdout, stderr = ssh.exec_command(cmd)
    return stdout.read().decode('utf-8', errors='replace').strip(), stderr.read().decode('utf-8', errors='replace').strip()

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(server["ip"], username=username, password=password, timeout=10)
    
    for topic in topics:
        cmd = f"/opt/module/kafka/bin/kafka-topics.sh --describe --bootstrap-server 192.168.45.100:9092 --topic {topic}"
        out, err = run_cmd(ssh, cmd)
        print(f"--- {topic} ---\n{out}")
        
except Exception as e:
    print(f"Error: {e}")
finally:
    ssh.close()
