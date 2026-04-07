# -*- coding: utf-8 -*-
"""
Step 5: 等待窗口聚合 + 验证 var_temp 和 kurtosis_temp 计算结果
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
print("  Step 5: 等待窗口聚合 + 验证计算结果")
print("="*60)

# 1. 先确认数据文件存在且有新内容
print("\n[1] 确认 /data_log/ 日志文件状态...")
out = run("ls -la /data_log/*.log 2>/dev/null", 5)
print(out)
out = run("tail -2 /data_log/device_state_generated.log /data_log/highfreq_sensor_generated.log /data_log/sensor_metrics_generated.log 2>/dev/null", 5)
print(f"\n最新日志:\n{out}")

# 2. 确认Kafka持续有新数据流入
print("\n[2] Kafka实时数据流检查(等待5秒)...")
time.sleep(5)
for topic in ['device_state', 'highfreq_sensor']:
    out = run(f"/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic {topic} --from-latest --timeout-ms 3000 2>/dev/null", 20)
    if out and out.strip() and len(out.strip()) > 3 and 'ERROR' not in out:
        print(f"[OK] {topic} 实时数据: {out.strip()[:120]}...")
    else:
        # 检查offset是否在增长
        out2 = run(f"/opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list master:9092 --topic {topic} --time -1 2>/dev/null", 15)
        print(f"[INFO] {topic} 当前offset: {out2.strip()}")

# 3. 确认Flink作业仍在运行
print("\n[3] Flink作业状态...")
out = run("export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flink && bin/flink list 2>&1", 20)
print(out)

# 4. 开始等待窗口聚合 (3分钟窗口，至少等待3-4分钟)
print("\n" + "="*60)
print("  [4] 等待3分钟窗口聚合完成...")
print("  窗口大小: 3 分钟 (TumblingWindow)")
print("="*60)

total_wait = 200  # 秒
for i in range(total_wait // 20):
    time.sleep(20)
    remaining = total_wait - (i+1)*20
    pct = ((i+1)*20) / total_wait * 100
    print(f"  [{pct:.0f}%] 已等待 {(i+1)*20}s, 剩余约 {remaining}s...")

print(f"\n  [OK] 等待完成! 共等待 {total_wait}秒")

# 5. 搜索var_temp和kurtosis_temp计算结果
print("\n" + "="*60)
print("  [5] 搜索 var_temp 和 kurtosis_temp 计算结果")
print("="*60)

# 在TaskManager日志中搜索
print("\n--- TaskManager 日志 ---")
out = run("grep -i 'var_temp\\|kurtosis_temp' /opt/module/flink/log/flink-root-taskexecutor-*-master.log 2>/dev/null | tail -30", 15)
if out.strip():
    print("[!!!] 找到 var_temp / kurtosis_temp 结果:")
    for line in out.strip().split('\n'):
        print(f"  {line.strip()[:200]}")
else:
    print("TaskManager日志中未找到")

# 在StandaloneSession日志中搜索
print("\n--- JobManager 日志 ---")
out = run("grep -i 'var_temp\\|kurtosis_temp' /opt/module/flink/log/flink-root-standalonesession-*-master.log 2>/dev/null | tail -30", 15)
if out.strip():
    print("[!!!] 找到 var_temp / kurtosis_temp 结果:")
    for line in out.strip().split('\n'):
        print(f"  {line.strip()[:200]}")
else:
    print("JobManager日志中未找到")

# 在所有flink日志中搜索
print("\n--- 所有Flink日志 ---")
out = run("grep -ri 'var_temp\\|kurtosis_temp' /opt/module/flink/log/ 2>/dev/null | tail -40", 15)
if out.strip():
    print("[!!!] 找到结果:")
    for line in out.strip().split('\n')[:25]:
        print(f"  {line.strip()[:200]}")
else:
    print("所有Flink日志中均未找到var_temp/kurtosis_temp关键字")

# 6. 扩展搜索：搜索其他特征值相关关键字
print("\n" + "="*60)
print("  [6] 扩展搜索: 所有特征值/报警/输出相关记录")
print("="*60)

keywords_search = [
    ('fft_peak', 'FFT峰值'),
    ('spindle_load', '主轴负载'),
    ('running_ratio', '运行率'),
    ('avg_feed_rate', '进给速率'),
    ('window', '窗口'),
    ('warning_log', '报警输出'),
    ('alarmMessage', '报警信息'),
    ('sink', '输出Sink'),
    ('feature', '特征'),
]

all_results = []
for kw, desc in keywords_search:
    out = run(f"grep -ri '{kw}' /opt/module/flink/log/flink-root-taskexecutor-*-master.log 2>/dev/null | tail -5", 10)
    if out.strip() and len(out.strip()) > 3:
        all_results.append((desc, kw, out))

if all_results:
    for desc, kw, res in all_results:
        print(f"\n>>> {desc} ({kw}):")
        for line in res.strip().split('\n')[:5]:
            print(f"  {line.strip()[:180]}")
else:
    print("[WARN] 未找到任何特征值计算记录")

# 7. 检查Flink作业是否有报错
print("\n" + "="*60)
print("  [7] Flink作业错误检查")
print("="*60)

out = run("grep -iE 'error|exception|fail|fatal' /opt/module/flink/log/flink-root-standalonesession-*-master.log 2>/dev/null | grep -v 'DEBUG\\|TRACE\\|expected' | tail -15", 10)
if out.strip():
    print("发现错误:")
    for line in out.strip().split('\n'):
        print(f"  {line.strip()[:180]}")
else:
    print("[OK] JobManager日志无错误")

out = run("grep -iE 'error|exception|fail|fatal' /opt/module/flink/log/flink-root-taskexecutor-*-master.log 2>/dev/null | grep -v 'DEBUG\\|TRACE\\|expected' | tail -15", 10)
if out.strip():
    print("\nTaskManager错误:")
    for line in out.strip().split('\n'):
        print(f"  {line.strip()[:180]}")
else:
    print("[OK] TaskManager日志无错误")

# 8. 最终汇总
print("\n" + "="*60)
print("  [8] 最终汇总")
print("="*60)

out = run("export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flink && bin/flink list 2>&1", 20)
print(f"Flink作业:\n{out}")

# Kafka offset变化（对比之前）
print("\nKafka最终数据量:")
for topic in ['device_state', 'highfreq_sensor', 'sensor_metrics']:
    out = run(f"/opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list master:9092 --topic {topic} --time -1 2>/dev/null", 15)
    if out.strip():
        offsets = [x.split(':')[-1] for x in out.strip().split('\n')]
        total = sum(int(o) for o in offsets if o.isdigit())
        print(f"  {topic}: 总消息数≈{total}, 分区详情={out.strip()}")

# warning_log topic（如果有报警数据会写到这里）
out = run(f"/opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list master:9092 --topic warning_log --time -1 2>/dev/null", 15)
if out.strip() and int(out.strip().split(':')[-1]) > 0:
    print(f"  warning_log(报警): {out.strip()}")
    # 获取一条报警数据看看
    out2 = run("/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic warning_log --from-latest --timeout-ms 3000 2>/dev/null", 15)
    if not out2 or not out2.strip():
        out2 = run("/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic warning_log --offset earliest --max-messages 1 2>/dev/null", 15)
    if out2 and out2.strip():
        print(f"  报警示例: {out2.strip()[:200]}")
else:
    print(f"  warning_log(报警): 无数据或不存在")

print("\n" + "="*60)
print("  Step 5 完成!")
print("="*60)

ssh.close()
