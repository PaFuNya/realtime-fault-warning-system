# -*- coding: utf-8 -*-
"""
Step 4: 检查数据流 + 等待窗口结果
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
        return f"[ERROR] {str(e)}"

print("="*60)
print("  Step 4: 检查数据流 + 等待窗口计算结果")
print("="*60)

# 1. 查看完整Flume配置，找到监控的日志路径
print("\n[1] 完整Flume配置(MyFlume.conf)...")
out = run("cat /opt/module/flume/conf/MyFlume.conf", 5)
print(out)

# 2. 找到实际的数据生成日志位置
print("\n[2] 数据生成脚本输出的日志位置...")
out = run("find /opt/module -name '*generated*.log' -o -name 'device_state*.log' -o -name 'highfreq*.log' 2>/dev/null", 15)
print(f"找到的日志文件:\n{out if out.strip() else '未找到'}")

# 3. 检查数据生成脚本的输出目录
print("\n[3] 检查数据生成脚本的工作方式...")
out = run("head -30 /opt/module/flume/conf/generate_highfreq_sensor.sh 2>/dev/null", 5)
if not out.strip():
    out = run("find /opt/module -name 'generate_highfreq*.sh' 2>/dev/null | head -1 | xargs cat 2>/dev/null | head -40", 5)
print(out)

# 4. 检查Flume运行状态和日志
print("\n[4] Flume运行状态...")
out = run("ps aux | grep flume-ng | grep -v grep", 5)
print(f"Flume进程:\n{out if out.strip() else '未运行'}")

# Flume最近日志
out = run("tail -30 /opt/module/flume/logs/flume.log 2>/dev/null || tail -30 /opt/module/flume/log/flume*.log 2>/dev/null || echo '无Flume日志'", 5)
print(f"\nFlume日志(最近30行):\n{out}")

# 5. 手动检查Kafka中各topic的数据量
print("\n[5] Kafka topic数据量检查...")
for topic in ['device_state', 'highfreq_sensor', 'sensor_metrics', 'lowfreq_sensor']:
    # 获取offset信息来判断是否有数据
    out = run(f"/opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list master:9092 --topic {topic} --time -1 2>/dev/null", 15)
    if out.strip():
        print(f"{topic}: offset={out.strip()}")
    else:
        print(f"{topic}: 无数据/不存在")
    
    # 尝试消费一条看看格式
    out2 = run(f"/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic {topic} --offset earliest --max-messages 1 --property print.timestamp=true 2>/dev/null", 20)
    if out2 and out2.strip() and len(out2.strip()) > 5:
        print(f"  示例数据: {out2.strip()[:150]}")

# 6. 检查Flink作业是否在接收数据
print("\n[6] Flink作业详细状态...")
out = run("export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flink && bin/flink list 2>&1", 20)
print(out)

# 7. Flink TaskManager日志 - 查看是否有数据处理记录
print("\n[7] Flink TaskManager日志 (搜索数据处理相关)...")
out = run("grep -E 'record|element|process|kafka|read|receive|input|output|sink|window|feature|var_temp|kurtosis|alarm' /opt/module/flink/log/flink-root-taskexecutor-*-master.log 2>/dev/null | tail -30", 10)
if out.strip():
    print(f"[OK] 找到数据处理记录:\n{out}")
else:
    print("[INFO] 日志中暂无数据处理关键字(可能还在初始化)")
    
    # 打印最近的TaskManager日志
    out2 = run("tail -50 /opt/module/flink/log/flink-root-taskexecutor-*-master.log 2>/dev/null", 5)
    if out2.strip():
        print(f"\nTaskManager最新日志:\n{out2[-500:] if len(out2)>500 else out2}")

# 8. StandaloneSession日志
print("\n[8] JobManager最新日志...")
out = run("tail -80 /opt/module/flink/log/flink-root-standalonesession-*-master.log 2>/dev/null", 5)
if out.strip():
    lines = [l for l in out.strip().split('\n') if l.strip()]
    key_lines = [l for l in lines if any(k in l.lower() for k in ['job', 'source', 'kafka', 'start', 'running', 'error', 'exception', 'warn', 'feature', 'window', 'task', 'operator'])]
    if key_lines:
        print("关键日志:")
        for l in key_lines[-20:]:
            print(f"  {l.strip()[:150]}")
    else:
        print(f"最新日志({len(lines)}行):")
        for l in lines[-10:]:
            print(f"  {l.strip()[:120]}")

ssh.close()
