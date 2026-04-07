# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
全程自动部署脚本 - 一步到位
1. 检查状态
2. 确保数据生成脚本运行
3. 确保Flume运行
4. 确保Kafka topics存在
5. 清理并重启Flink
6. 提交作业
7. 验证结果
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
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode('utf-8', errors='replace')
    err = stderr.read().decode('utf-8', errors='replace')
    return out + err

print("="*60)
print("  全程自动部署 - 开始")
print("="*60)

# ========== Step 1: 检查当前状态 ==========
print("\n[Step 1] 检查服务器当前状态...")
print("-"*40)

# 检查数据生成脚本
out = run("ps aux | grep -E 'generate_change|generate_highfreq' | grep -v grep")
if 'generate' in out.lower() or 'sh' in out:
    print("✓ 数据生成脚本正在运行:")
    for line in out.strip().split('\n')[:5]:
        if line:
            print(f"  {line[:100]}")
else:
    print("⚠ 数据生成脚本未运行，需要启动")

# 检查Flume
out = run("ps aux | grep flume | grep -v grep | head -3")
flume_count = len([l for l in out.strip().split('\n') if l and 'flume' in l.lower()])
print(f"\n✓ Flume进程数: {flume_count}")

# 检查日志文件更新
out = run("ls -la /opt/module/flume/conf/*.log 2>/dev/null; tail -2 /opt/module/flume/conf/highfreq_sensor_generated.log /opt/module/flume/conf/device_state_generated.log 2>/dev/null")
print(f"✓ 日志文件状态:\n{out[-300:] if len(out)>300 else out}")

# 检查Kafka topics
out = run("/opt/module/kafka/bin/kafka-topics.sh --bootstrap-server master:9092 --list 2>/dev/null")
print(f"\n✓ Kafka Topics: {out.strip()}")

# 检查Flink
out = run("jps | grep -E 'StandaloneSession|TaskManager'")
print(f"✓ Flink进程:\n{out if out.strip() else '  无'}")

# 检查端口占用
out = run("netstat -tlnp 2>/dev/null | grep -E '8081|8082|8083|6123' | head -5")
print(f"✓ 关键端口:\n{out if out.strip() else '  无占用'}")

# ========== Step 2: 启动数据生成脚本（如果需要） ==========
print("\n[Step 2] 确保数据生成脚本在运行...")
out = run("ps aux | grep -E 'generate_change|generate_highfreq' | grep -v grep | wc -l")
if int(out.strip()) < 2:
    print("  启动数据生成脚本...")
    run("cd /opt/module/flume/conf && nohup bash generate_change_data.sh > /dev/null 2>&1 &")
    run("cd /opt/module/flume/conf && nohup bash generate_highfreq_sensor.sh > /dev/null 2>&1 &")
    time.sleep(2)
    print("  ✓ 已启动")
else:
    print("  ✓ 数据生成脚本已在运行")

# ========== Step 3: 确保Flume运行 ==========
print("\n[Step 3] 确保Flume在运行...")
out = run("ps aux | grep flume | grep -v grep | wc -l")
if int(out.strip()) == 0:
    print("  启动Flume...")
    run("export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flume && nohup bin/flume-ng agent -c conf/ -f conf/MyFlume.conf -n a1 -Dflume.root.logger=INFO,console > /dev/null 2>&1 &")
    time.sleep(5)
    print("  ✓ Flume已启动")
else:
    print(f"  ✓ Flume已在运行 ({out.strip()}个进程)")

# ========== Step 4: 确保Kafka Topics存在 ==========
print("\n[Step 4] 确保Kafka Topics存在...")
topics_to_create = ['highfreq_sensor', 'lowfreq_sensor', 'device_state']
existing = run("/opt/module/kafka/bin/kafka-topics.sh --bootstrap-server master:9092 --list 2>/dev/null").strip().split('\n')

for topic in topics_to_create:
    if topic not in existing:
        print(f"  创建topic: {topic}")
        run(f"/opt/module/kafka/bin/kafka-topics.sh --bootstrap-server master:9092 --create --partitions 3 --replication-factor 1 --topic {topic} 2>/dev/null")
    else:
        print(f"  ✓ {topic} 已存在")

# 检查Kafka中是否有新数据
time.sleep(3)
print("\n  检查Kafka数据流入...")
out = run("/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic highfreq_sensor --from-latest --timeout-ms 5000 2>/dev/null")
if out and out.strip():
    print(f"  ✓ highfreq_sensor 有数据: {out.strip()[:150]}...")
else:
    # 尝试从头取几条看是否有数据
    out = run("/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic highfreq_sensor --offset earliest --max-messages 1 2>/dev/null")
    if out and out.strip():
        print(f"  ⚠ Kafka有旧数据(可能无新数据): {out.strip()[:150]}...")
    else:
        print(f"  ⚠ highfreq_sensor topic为空")

# ========== Step 5: 清理并重启Flink ==========
print("\n[Step 5] 清理并重启Flink集群...")

# 先杀掉所有Flink进程
run("pkill -9 -f StandaloneSession 2>/dev/null; pkill -9 -f TaskManager 2>/dev/null; pkill -9 -f flink 2>/dev/null")
time.sleep(3)

# 确认清理干净
out = run("jps | grep -E 'StandaloneSession|TaskManager'")
if out.strip():
    print(f"  ⚠ 还有残留Flink进程: {out.strip()}")
else:
    print("  ✓ Flink进程已清理")

# 杀掉占用8080-8090端口的进程（可能是旧的Flink Web UI）
run("for port in 8080 8081 8082 8083 8084 8085; do pid=$(netstat -tlnp 2>/dev/null | grep :$port | awk '{print $7}' | cut -d'/' -f1); if [ -n \"$pid\" ] && [ \"$pid\" != \"-\" ]; then kill -9 $pid 2>/dev/null && echo \"  killed $pid on port $port\"; fi; done")
time.sleep(2)

# 启动Flink集群
print("  启动Flink集群...")
run("export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flink && bin/start-cluster.sh 2>&1")
time.sleep(8)

# 检查启动结果
out = run("jps | grep -E 'StandaloneSession|TaskManager'")
print(f"  Flink进程:\n{out if out.strip() else '  ✗ 未启动!'}")

# 检查Web UI端口
out = run("netstat -tlnp 2>/dev/null | grep -E '808[0-9]' | head -3")
print(f"  Web UI端口:\n{out if out.strip() else '  未监听'}")

# ========== Step 6: 提交Flink作业 ==========
print("\n[Step 6] 提交Flink作业...")

# 检查JAR文件是否存在
jar_path = "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
out = run(f"ls -la {jar_path} 2>/dev/null")
if jar_path in out or 'realtime-data-process' in out:
    print(f"  ✓ JAR文件存在")
    
    # 提交作业（使用detached模式）
    print("  提交Flink作业(detached模式)...")
    submit_out = run("export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flink && bin/flink run -d realtime-data-process-1.0-SNAPSHOT.jar 2>&1", timeout=60)
    print(f"  提交结果:\n{submit_out[-500:] if len(submit_out) > 500 else submit_out}")
else:
    print(f"  ✗ JAR文件不存在: {jar_path}")

time.sleep(5)

# 检查作业列表
print("\n  检查作业状态...")
out = run("export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flink && bin/flink list 2>&1", timeout=15)
print(f"  {out[-400:] if len(out) > 400 else out}")

# ========== Step 7: 验证计算结果 ==========
print("\n[Step 7] 等待窗口聚合并验证var_temp和kurtosis_temp...")
print("(等待3分钟让窗口积累数据...)")

# 等待3分钟
for i in range(6):
    time.sleep(30)
    remaining = 180 - (i+1)*30
    print(f"  等待中... 还剩 {remaining}秒")

# 检查Flink日志中的计算结果
print("\n  搜索var_temp计算结果...")
out = run("grep -r 'var_temp\\|kurtosis_temp\\|窗口聚合\\|特征值' /opt/module/flink/log/ 2>/dev/null | tail -20")
if out.strip():
    print(f"  ✓ 找到计算结果:\n{out}")
else:
    print("  日志中未找到var_temp/kurtosis_temp输出")

# 再次检查作业是否还在运行
print("\n  最终检查作业状态...")
out = run("jps | grep -E 'StandaloneSession|TaskManager'")
print(f"  Flink进程:\n{out if out.strip() else '  ✗ Flink未运行!'}")

out = run("export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flink && bin/flink list 2>&1", timeout=15)
print(f"  作业列表:\n{out[-300:] if len(out) > 300 else out}")

# 检查最新日志
print("\n  最新Flink日志:")
out = run("tail -50 /opt/module/flink/log/flink-*-standalonesession-*.log 2>/dev/null || tail -50 /opt/module/flink/log/*.log 2>/dev/null")
print(f"{out[-500:] if len(out) > 500 else out}")

print("\n" + "="*60)
print("  全程部署完成！")
print("="*60)

ssh.close()
