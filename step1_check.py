# -*- coding: utf-8 -*-
"""
Step 1: 检查服务器当前状态 + 启动缺失服务
"""
import paramiko
import time
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)

def run(cmd, timeout=30):
    try:
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
        out = stdout.read().decode('utf-8', errors='replace')
        err = stderr.read().decode('utf-8', errors='replace')
        return out + err
    except Exception as e:
        return f"[TIMEOUT/ERROR] {str(e)}"

print("="*60)
print("  Step 1: 检查服务器状态并确保所有服务运行")
print("="*60)

# 1. 数据生成脚本
print("\n--- 数据生成脚本 ---")
out = run("ps aux | grep -E 'generate_change|generate_highfreq' | grep -v grep | wc -l")
count = int(out.strip()) if out.strip().isdigit() else 0
print(f"运行中的数据生成脚本: {count}个")
if count < 2:
    print("启动数据生成脚本...")
    run("cd /opt/module/flume/conf && nohup bash generate_change_data.sh > /dev/null 2>&1 &", 5)
    run("cd /opt/module/flume/conf && nohup bash generate_highfreq_sensor.sh > /dev/null 2>&1 &", 5)
    print("已启动!")
else:
    print("[OK] 数据生成脚本已在运行")

# 2. Flume
print("\n--- Flume ---")
out = run("ps aux | grep flume | grep -v grep | wc -l", 5)
flume_count = int(out.strip()) if out.strip().isdigit() else 0
print(f"Flume进程数: {flume_count}")
if flume_count == 0:
    print("启动Flume...")
    cmd = "export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flume && nohup bin/flume-ng agent -c conf/ -f conf/MyFlume.conf -n a1 > /dev/null 2>&1 &"
    run(cmd, 5)
    time.sleep(3)
    print("Flume启动命令已发送")
else:
    print("[OK] Flume已在运行")

# 3. 日志文件
print("\n--- 日志文件最新内容 ---")
out = run("tail -1 /opt/module/flume/conf/highfreq_sensor_generated.log; tail -1 /opt/module/flume/conf/device_state_generated.log", 5)
print(out)

# 4. Kafka topics
print("\n--- Kafka Topics ---")
out = run("/opt/module/kafka/bin/kafka-topics.sh --bootstrap-server master:9092 --list 2>/dev/null", 10)
topics = out.strip().split('\n') if out.strip() else []
print(f"现有Topics: {topics}")

for t in ['highfreq_sensor', 'lowfreq_sensor', 'device_state']:
    if t not in topics:
        print(f"创建topic: {t}")
        run(f"/opt/module/kafka/bin/kafka-topics.sh --bootstrap-server master:9092 --create --partitions 3 --replication-factor 1 --topic {t} 2>/dev/null", 10)

# 5. Kafka是否有数据
print("\n--- Kafka数据检查 ---")
out = run("/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic highfreq_sensor --from-latest --timeout-ms 3000 2>/dev/null", 15)
if out and out.strip():
    print(f"[OK] highfreq_sensor有新数据: {out.strip()[:120]}...")
else:
    print("highfreq_sensor暂无新数据流入(可能需要等待)")

# 6. Flink进程
print("\n--- Flink进程 ---")
out = run("jps | grep -E 'StandaloneSession|TaskManager'", 5)
if out.strip():
    print(f"Flink正在运行:\n{out}")
else:
    print("Flink未运行，需要启动")

# 7. 端口占用
print("\n--- 关键端口 ---")
out = run("netstat -tlnp 2>/dev/null | grep -E '808[0-9]|6123' | head -8", 5)
print(out if out.strip() else "无特殊端口占用")

print("\n" + "="*60)
print("  Step 1 完成! 接下来执行 Step 2 (清理+重启Flink)")
print("="*60)
ssh.close()
