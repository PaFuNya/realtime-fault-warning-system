# -*- coding: utf-8 -*-
"""
Step 2: 清理Flink + 重启集群 + 提交作业
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
print("  Step 2: 清理Flink + 重启动 + 提交作业")
print("="*60)

# 1. 先检查日志文件实际位置
print("\n[1] 查找日志文件位置...")
out = run("find /opt/module -name '*generated*.log' -o -name 'highfreq*.log' 2>/dev/null | head -5", 10)
print(f"日志文件:\n{out}")

out = run("find /opt/module -name '*.log' -newer /opt/module/flume/conf/MyFlume.conf -mmin -60 2>/dev/null | head -10", 15)
print(f"最近更新的log文件:\n{out if out.strip() else '无'}")

# 2. 检查Flume配置中的监控路径
print("\n[2] Flume配置中的监控路径...")
out = run("grep -E 'tail.*Path|spoolDir|file.*log' /opt/module/flume/conf/MyFlume.conf | head -10", 5)
print(out)

# 3. 检查Kafka数据流入
print("\n[3] Kafka数据检查(等待新数据)...")
time.sleep(5)  # 等5秒让Flume发送数据
for topic in ['device_state', 'highfreq_sensor']:
    out = run(f"/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic {topic} --from-latest --timeout-ms 3000 2>/dev/null", 20)
    if out and out.strip() and 'ERROR' not in out and 'Exception' not in out:
        print(f"[OK] {topic}: {out.strip()[:120]}...")
    else:
        print(f"[WARN] {topic}: 无新数据或超时")

# 4. 清理旧Flink进程
print("\n[4] 清理旧的Flink进程...")
run("pkill -9 -f flink 2>/dev/null", 5)
run("pkill -9 -f StandaloneSession 2>/dev/null", 5)
run("pkill -9 -f TaskManager 2>/dev/null", 5)
time.sleep(3)

# 杀掉占用8081-8090的java进程（可能是旧的Flink Web UI）
print("清理端口占用...")
run("""for port in 8080 8081 8082 8083 8084 8085; do pid=$(ss -tlnp 2>/dev/null | grep ":$port " | grep -oP 'pid=\\K[0-9]+'); if [ -n "$pid" ]; then kill -9 $pid 2>/dev/null && echo "killed $pid on port $port"; fi; done""", 10)
time.sleep(2)

# 确认清理完成
out = run("ps aux | grep -E 'StandaloneSession|TaskManager|flink.*java' | grep -v grep", 5)
if out.strip():
    print(f"残留进程:\n{out}")
else:
    print("[OK] Flink进程已全部清理")

# 5. 启动Flink集群
print("\n[5] 启动Flink集群...")
out = run("export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flink && bin/start-cluster.sh 2>&1", 30)
print(f"启动输出:\n{out[-400:] if len(out)>400 else out}")
time.sleep(8)

# 6. 验证Flink启动
print("\n[6] 验证Flink启动状态...")
out = run("ps aux | grep -E 'flink.*conf.*Standalone|TaskManagerRunner' | grep -v grep | head -5", 5)
if out.strip():
    print(f"[OK] Flink进程已启动:\n{out}")
else:
    # 尝试用其他方式检查
    out = run("ss -tlnp 2>/dev/null | grep -E '6123|808'", 5)
    print(f"Flink端口监听:\n{out if out.strip() else '[FAIL] 未检测到Flink进程/端口'}")

# 7. 检查JAR文件
print("\n[7] 检查JAR文件...")
out = run("ls -la /opt/module/flink/*.jar /opt/module/flink/userlib/*.jar 2>/dev/null", 5)
print(out)

# 如果没有找到JAR，搜索一下
if 'realtime-data-process' not in out:
    out = run("find /opt/module -name 'realtime-data*.jar' 2>/dev/null", 10)
    if not out.strip():
        print("[WARN] JAR未找到! 需要上传!")
    else:
        print(f"[OK] JAR位置: {out.strip()}")

# 8. 提交Flink作业
print("\n[8] 提交Flink作业(detached模式)...")
submit_cmd = """export JAVA_HOME=/opt/module/jdk1.8.0_212 && \
cd /opt/module/flink && \
bin/flink run -d realtime-data-process-1.0-SNAPSHOT.jar 2>&1"""

print("提交命令执行中 (可能需要30-60秒)...")
out = run(submit_cmd, timeout=120)
result = out[-600:] if len(out) > 600 else out
print(f"\n提交结果:\n{result}")

time.sleep(5)

# 9. 检查作业列表
print("\n[9] 检查Flink作业列表...")
list_cmd = "export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flink && bin/flink list 2>&1"
out = run(list_cmd, timeout=30)
print(f"{out[-400:] if len(out) > 400 else out}")

# 10. 最终状态汇总
print("\n" + "="*60)
print("  Step 2 完成! 最终状态")
print("="*60)

# Flink是否在运行
out = run("ps aux | grep -E 'flink.*conf.*Standalone|TaskManagerRunner' | grep -v grep | wc -l", 5)
print(f"Flink进程数: {out.strip()}")

# 端口
out = run("ss -tlnp 2>/dev/null | grep -E '6123|808'", 5)
print(f"端口:\n{out if out.strip() else '无'}")

# Kafka数据验证
out = run("/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic device_state --from-latest --timeout-ms 2000 2>/dev/null", 15)
print(f"device_state最新数据: {'有' + out.strip()[0:80] if out.strip() and 'ERROR' not in out else '暂无'}")

ssh.close()
