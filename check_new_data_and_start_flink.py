#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查新数据并启动Flink作业
"""

import paramiko
import time

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 检查新数据并启动Flink ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 检查日志文件的最新时间戳
    print("\n=== 1. 日志文件最新时间戳 ===")
    stdin, stdout, stderr = ssh.exec_command("tail -1 /data_log/highfreq_sensor_generated.log | grep -o 'timestamp\":\"[^\"]*\"'")
    latest_ts = stdout.read().decode().strip()
    print(f"highfreq日志最新时间戳: {latest_ts}")
    
    # 2. 等待新数据进入Kafka
    print("\n=== 2. 等待新数据进入Kafka (5秒) ===")
    time.sleep(5)
    
    # 只消费最新的数据（不从开头）
    stdin, stdout, stderr = ssh.exec_command(
        "/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic highfreq_sensor --max-messages 3 --timeout-ms 5000 2>/dev/null"
    )
    new_data = stdout.read().decode().strip()
    if new_data:
        print(f"[OK] 收到新数据 ({len(new_data.split(chr(10)))} 条):")
        for line in new_data.split('\n')[:2]:
            ts = line.split('timestamp\":\"')[1].split('\"')[0] if 'timestamp' in line else 'N/A'
            mid = line.split('machineId\":')[1].split(',')[0] if 'machineId' in line else 'N/A'
            print(f"  设备{mid} @ {ts}")
    else:
        print("[WARN] 暂无新数据，可能Flume在消费旧数据")
    
    # 3. 启动Flink集群
    print("\n=== 3. 启动Flink集群 ===")
    stdin, stdout, stderr = ssh.exec_command("jps | grep StandaloneSession")
    if not stdout.read().decode().strip():
        print("Flink未运行，正在启动...")
        # 修改端口避免冲突
        ssh.exec_command("sed -i 's/^rest.port:.*/rest.port: 8083/' /opt/module/flink/conf/flink-conf.yaml 2>/dev/null || echo 'rest.port: 8083' >> /opt/module/flink/conf/flink-conf.yaml")
        ssh.exec_command("/opt/module/flink/bin/start-cluster.sh")
        time.sleep(5)
        
        stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'StandaloneSession|TaskManager'")
        flink_processes = stdout.read().decode().strip()
        print(f"Flink进程:\n{flink_processes}")
    else:
        print("Flink已在运行")
    
    # 4. 检查并提交Flink作业
    print("\n=== 4. 提交Flink作业 ===")
    
    # 检查现有作业
    stdin, stdout, stderr = ssh.exec_command("/opt/module/flink/bin/flink list 2>/dev/null")
    jobs = stdout.read().decode()
    print(f"当前作业:\n{jobs}")
    
    # 取消现有作业
    if "RUNNING" in jobs or "CREATED" in jobs:
        print("\n取消现有作业...")
        import re
        job_ids = re.findall(r'([a-f0-9]{32})', jobs)
        for job_id in job_ids:
            ssh.exec_command(f"/opt/module/flink/bin/flink cancel {job_id}")
            time.sleep(1)
    
    # 检查JAR文件
    jar_file = "/opt/module/flink/OverMatch-1.0-SNAPSHOT.jar"
    stdin, stdout, stderr = ssh.exec_command(f"ls -la {jar_file}")
    if stdout.read().decode().strip():
        print(f"\n找到JAR文件: {jar_file}")
        
        # 提交作业
        cmd = f"/opt/module/flink/bin/flink run -d {jar_file} 2>&1"
        print(f"提交作业: {cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd)
        result = stdout.read().decode()
        print(result)
        
        time.sleep(3)
        stdin, stdout, stderr = ssh.exec_command("/opt/module/flink/bin/flink list")
        print(f"\n作业状态:\n{stdout.read().decode()}")
    else:
        print(f"\n[WARN] JAR文件不存在: {jar_file}")
        print("需要上传JAR文件到服务器")
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
