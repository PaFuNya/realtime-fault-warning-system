# -*- coding: utf-8 -*-
"""
Step 3: 用nohup后台方式提交Flink作业 + 验证结果
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
print("  Step 3: 后台提交Flink作业 + 验证")
print("="*60)

# 1. 先检查Flink REST端口配置 - 改用其他端口
print("\n[1] 检查并修改Flink Web UI端口...")
out = run("cat /opt/module/flink/conf/flink-conf.yaml | grep rest.port | head -3", 5)
print(f"当前rest.port: {out}")

# 修改Flink REST端口到8090避免冲突
run("""sed -i 's/rest.port: .*/rest.port: 8090/' /opt/module/flink/conf/flink-conf.yaml""", 5)

# 验证修改
out = run("grep 'rest.port' /opt/module/flink/conf/flink-conf.yaml | head -1", 5)
print(f"修改后: {out}")

# 2. 重启Flink集群使新配置生效
print("\n[2] 重启Flink集群(使用新端口)...")
run("pkill -9 -f StandaloneSession 2>/dev/null", 5)
run("pkill -9 -f TaskManagerRunner 2>/dev/null", 5)
time.sleep(3)
run("export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flink && bin/start-cluster.sh 2>&1", 30)
time.sleep(10)

# 确认新端口在监听
out = run("ss -tlnp 2>/dev/null | grep 8090", 5)
print(f"8090端口: {'[OK] 已监听' if '8090' in out else '[WARN] 未监听'}")

out = run("ss -tlnp 2>/dev/null | grep 6123", 5)
print(f"6123端口: {'[OK] 已监听' if '6123' in out else '[FAIL] 未监听'}")

# 3. 提交Flink作业 (使用-d detached模式)
print("\n[3] 提交Flink作业(detached模式)...")
submit_cmd = """export JAVA_HOME=/opt/module/jdk1.8.0_212 && \
cd /opt/module/flink && \
nohup bin/flink run -d realtime-data-process-1.0-SNAPSHOT.jar > /tmp/flink_submit.log 2>&1 &"""
run(submit_cmd, 15)
print("提交命令已发送，等待20秒...")
time.sleep(20)

# 检查提交日志
out = run("cat /tmp/flink_submit.log 2>/dev/null", 5)
if out.strip():
    print(f"提交日志:\n{out[-500:] if len(out)>500 else out}")
else:
    print("提交日志为空")

# 4. 检查作业列表（用新端口）
print("\n[4] 检查Flink作业状态...")
list_cmd = "export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flink && bin/flink list 2>&1"
for attempt in range(3):
    time.sleep(5)
    out = run(list_cmd, timeout=20)
    print(f"\n--- 第{attempt+1}次尝试 ---")
    result = out[-400:] if len(out) > 400 else out
    print(result)
    
    # 检查是否有运行中的作业
    if 'RUNNING' in out or 'running' in out.lower() or 'JobId' in out:
        print("[OK] 检测到Flink作业!")
        break

# 5. 如果flink list不工作，直接看进程和日志判断
print("\n[5] 直接检查作业是否在运行...")
# 检查Flink日志中是否有作业相关信息
out = run("tail -100 /opt/module/flink/log/flink-root-standalonesession-*-master.log 2>/dev/null | tail -50", 10)
if out.strip():
    # 只打印关键信息
    lines = out.strip().split('\n')
    key_lines = [l for l in lines if any(k in l for k in ['job', 'Job', 'started', 'submitted', 'kafka', 'Kafka', 'window', 'feature', 'var_temp', 'error', 'Exception'])]
    if key_lines:
        print("关键日志行:")
        for l in key_lines[-15:]:
            print(f"  {l.strip()[:150]}")
    else:
        print("最近50行日志:")
        for l in lines[-15:]:
            print(f"  {l.strip()[:120]}")

# 6. 检查Kafka数据流入情况
print("\n[6] Kafka数据验证...")
time.sleep(2)
for topic in ['device_state', 'highfreq_sensor']:
    out = run(f"/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic {topic} --from-latest --timeout-ms 3000 2>/dev/null", 20)
    if out and out.strip() and 'ERROR' not in out and len(out.strip()) > 5:
        print(f"[OK] {topic}: {out.strip()[:100]}...")
    else:
        # 尝试从头获取一条确认数据存在
        out2 = run(f"/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic {topic} --offset earliest --max-messages 1 2>/dev/null", 15)
        if out2 and out2.strip():
            print(f"[INFO] {topic}: 有历史数据({len(out2.strip())}字节), 但无实时新数据")
        else:
            print(f"[WARN] {topic}: 无数据")

# 7. 最终汇总
print("\n" + "="*60)
print("  最终状态汇总")
print("="*60)

out = run("ps aux | grep -E 'StandaloneSession|TaskManagerRunner' | grep -v grep | wc -l", 5)
print(f"Flink进程数: {out.strip()}")

out = run("ss -tlnp 2>/dev/null | grep -E '8090|6123'", 5)
print(f"端口监听:\n{out if out.strip() else '无'}")

# 检查Flume和数据生成脚本
out = run("ps aux | grep -E 'generate_change|generate_highfreq|flume-ng' | grep -v grep | wc -l", 5)
print(f"数据服务(Flume+数据生成): {out.strip()}个进程")

ssh.close()
