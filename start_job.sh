#!/bin/bash
echo "=== 启动Flink作业 ==="

# 清理
pkill -9 -f StandaloneSession 2>/dev/null
pkill -9 -f TaskManager 2>/dev/null
sleep 3

# 启动Flink
cd /opt/module/flink
bin/start-cluster.sh
sleep 5

# 检查进程
echo "=== Flink进程 ==="
jps | grep -E 'StandaloneSession|TaskManager'

# 提交作业
echo "=== 提交作业 ==="
bin/flink run -d /opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar 2>&1 | tee /tmp/flink-submit.log

echo "=== 完成 ==="
echo "3分钟后检查计算结果: grep 'var_temp' /opt/module/flink/log/*.out"
