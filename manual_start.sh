#!/bin/bash
# 手动启动Flink作业脚本

echo "=== 清理Flink进程 ==="
pkill -9 -f StandaloneSession 2>/dev/null
pkill -9 -f TaskManager 2>/dev/null
sleep 3

echo "=== 启动Flink集群 ==="
cd /opt/module/flink
bin/start-cluster.sh
sleep 5

echo "=== 检查进程 ==="
jps | grep -E 'StandaloneSession|TaskManager'

echo "=== 提交Flink作业 ==="
JAR_FILE="/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
nohup bin/flink run -d $JAR_FILE > /var/log/flink-job.log 2>&1 &

echo "=== 等待10秒 ==="
sleep 10

echo "=== 检查作业状态 ==="
bin/flink list 2>/dev/null || echo "无法连接REST，检查日志..."

echo "=== 作业日志 ==="
tail -50 /var/log/flink-job.log

echo "=== 完成 ==="
echo "等待3分钟后检查var_temp和kurtosis_temp..."
