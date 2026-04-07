# -*- coding: utf-8 -*-
"""
查看Flink输出信息 - 所有可用的方式
"""
import paramiko
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

print("="*65)
print("  Flink输出信息 - 全部查看方式")
print("="*65)

# ========== 方式1: Flink Web UI ==========
print("\n" + "━"*65)
print("  方式1: Flink Web UI (可视化界面)")
print("━"*65)
out = run("hostname -I | awk '{print $1}'", 5)
ip = out.strip()
print(f"\n  浏览器打开: http://{ip}:8090")
print(f"\n  可以查看:")
print(f"    • Overview - 集群概览、作业列表")
print(f"    • 点击作业名 → Job Graph(数据流拓扑图)")
print(f"    → SubTasks → 各算子的输入/输出记录数、吞吐量")
print(f"    → Exceptions → 错误信息")
print(f"    → Checkpoints -> 状态快照")

# 检查Web UI是否正常
out = run(f"curl -s -o /dev/null -w '%{{http_code}}' http://localhost:8090/overview", 10)
print(f"\n  Web UI状态码: {out.strip()} ({'正常访问' if '200' in out else '可能异常'})")

# ========== 方式2: Flink CLI命令行 ==========
print("\n" + "━"*65)
print("  方式2: Flink CLI 命令行")
print("━"*65)

print("""
  SSH到服务器后执行:

  # 查看作业列表
  cd /opt/module/flink && bin/flink list

  # 取消作业
  bin/flink cancel <JobID>

  # 触发Savepoint(保存状态)
  bin/flink savepoint <JobID> /tmp/savepoints

  # 作业详情
  bin/flink info /opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar
""")

# 执行flink list看看当前状态
print("  当前作业状态:")
out = run("export JAVA_HOME=/opt/module/jdk1.8.0_212 && cd /opt/module/flink && bin/flink list 2>&1", 20)
print(f"{out}")

# ========== 方式3: Flink日志文件 (.out 和 .log) ==========
print("\n" + "━"*65)
print("  方式3: Flink日志文件 (核心输出在这里!)")
print("━"*65)
print("""
  ★★★ 最重要: .out 文件包含你的业务打印输出 ★★★

  日志文件位置: /opt/module/flink/log/
""")

# 列出所有日志文件
out = run("ls -lhS /opt/module/flink/log/*.out 2>/dev/null | head -15", 5)
if out.strip():
    print(f"\n  .out 文件 (业务输出):")
    print(out)

out = run("ls -lhS /opt/module/flink/log/*.log 2>/dev/null | head -15", 5)
if out.strip():
    print(f"\n  .log 文件 (系统日志):")
    print(out)

# 显示var_temp/kurtosis计算结果
print("\n  --- 实时计算结果 (var_temp / kurtosis_temp) ---")
out = run("grep -E 'var_temp|kurtosis_temp|窗口聚合|窗口特征' /opt/module/flink/log/*.out 2>/dev/null | tail -20", 10)
if out.strip():
    for line in out.strip().split('\n')[:15]:
        print(f"  {line.strip()[:200]}")
else:
    print("  (暂无新输出)")

# 显示报警信息
print("\n  --- 报警输出 (warning_log) ---")
out = run("grep -iE 'alarm|报警|RUL|fault' /opt/module/flink/log/*.out 2>/dev/null | tail -10", 10)
if out.strip():
    for line in out.strip().split('\n')[:8]:
        print(f"  {line.strip()[:180]}")
else:
    print("  (暂无报警输出)")

# 实时跟踪命令提示
print("""
  ★ 实时跟踪Flink输出 (推荐!):
  
  # 终端1: 跟踪TaskManager的.out文件(看到var_temp等计算结果)
  tail -f /opt/module/flink/log/flink-root-taskexecutor-*-master.out
  
  # 终端2: 跟踪JobManager的.out文件  
  tail -f /opt/module/flink/log/flink-root-standalonesession-*-master.out
  
  # 终端3: 只看窗口聚合结果(过滤关键字)
  tail -f /opt/module/flink/log/flink-root-taskexecutor-*-master.out | grep -E 'var_temp|kurtosis|窗口|报警'
""")

# ========== 方式4: Kafka消费端查看 ==========
print("\n" + "━"*65)
print("  方式4: Kafka消费 (查看Flink写入Kafka的数据)")
print("━"*65)

# 检查warning_log数据量
for topic, desc in [('device_state','低频设备状态'), ('highfreq_sensor','高频传感器'), ('sensor_metrics','传感器指标'), ('warning_log','⭐报警输出')]:
    out = run(f"/opt/module/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list master:9092 --topic {topic} --time -1 2>/dev/null", 10)
    if out.strip() and ':' in out.strip():
        offsets = [int(x.split(':')[-1]) for x in out.strip().split('\n') if x.split(':')[-1].isdigit()]
        total = sum(offsets) if offsets else 0
        print(f"  [{desc}] {topic}: ~{total}条")

# 获取最新几条报警数据
print("\n  --- 最新报警数据(warning_log) ---")
out = run("/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic warning_log --from-latest --timeout-ms 3000 --max-messages 3 2>/dev/null || /opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic warning_log --offset earliest --max-messages 3 2>/dev/null", 15)
if out and out.strip():
    lines = [l for l in out.strip().split('\n') if l.strip()]
    for line in lines[:3]:
        print(f"  {line[:250]}")
else:
    out = run("/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic warning_log --offset earliest --max-messages 3 2>/dev/null", 15)
    if out and out.strip():
        lines = [l for l in out.strip().split('\n') if l.strip()]
        for line in lines[:3]:
            print(f"  {line[:250]}")

print("""
  ★ 实时消费Kafka报警数据:
  
  # 持续查看报警输出(Flink的计算结果)
  /opt/module/kafka/bin/kafka-console-consumer.sh \\
      --bootstrap-server master:9092 \\
      --topic warning_log \\
      --from-latest
  
  # 从头开始看历史报警数据
  /opt/module/kafka/bin/kafka-console-consumer.sh \\
      --bootstrap-server master:9092 \\
      --topic warning_log \\
      --offset earliest
""")

# ========== 快速参考汇总 ==========
print("="*65)
print("  快速参考: 在服务器上执行的常用命令")
print("="*65)
print("""
  ╔═══════════════════════════════════════════════════════════╗
  ║  1. WebUI:   curl http://192.168.45.100:8090             ║
  ║  2. 作业列表: cd /opt/module/flink && bin/flink list     ║
  ║  3. 实时输出: tail -f /opt/module/flink/log/*.out        ║
  ║  4. 过滤结果: tail -f .../*.out | grep var_temp          ║
  ║  5. 报警数据: kafka-console-consumer --topic warning_log ║
  ╚═══════════════════════════════════════════════════════════╝
""")

ssh.close()
