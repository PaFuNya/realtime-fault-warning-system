#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查并修复所有问题：Flume数据流和Flink作业
"""

import paramiko
import time

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 全面检查和修复 ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 检查Flume日志
    print("\n=== 1. 检查Flume日志 ===")
    stdin, stdout, stderr = ssh.exec_command("tail -100 /var/log/flume-agent.log")
    log = stdout.read().decode()
    print(log[-2000:] if len(log) > 2000 else log)
    
    # 2. 检查Kafka topic是否存在
    print("\n=== 2. 检查Kafka topic ===")
    stdin, stdout, stderr = ssh.exec_command("kafka-topics.sh --list --bootstrap-server master:9092")
    topics = stdout.read().decode().strip()
    print(f"存在的topics:\n{topics}")
    
    # 检查需要的topic是否存在
    required_topics = ['highfreq_sensor', 'device_state', 'sensor_metrics']
    for topic in required_topics:
        if topic in topics:
            print(f"  [OK] {topic} 存在")
        else:
            print(f"  [MISSING] {topic} 不存在，需要创建")
            stdin, stdout, stderr = ssh.exec_command(
                f"kafka-topics.sh --create --topic {topic} --bootstrap-server master:9092 --partitions 3 --replication-factor 1 2>/dev/null"
            )
            print(f"  创建结果: {stdout.read().decode()}")
    
    # 3. 检查Flume进程
    print("\n=== 3. 检查Flume进程 ===")
    stdin, stdout, stderr = ssh.exec_command("ps aux | grep flume | grep -v grep")
    flume_process = stdout.read().decode().strip()
    if flume_process:
        print(f"Flume进程:\n{flume_process}")
    else:
        print("Flume未运行，需要重启")
    
    # 4. 手动测试数据能否发送到Kafka
    print("\n=== 4. 手动测试Kafka发送 ===")
    test_msg = '{"machineId":109,"timestamp":"2026-04-07 11:20:00","temperature":45.0}'
    stdin, stdout, stderr = ssh.exec_command(
        f"echo '{test_msg}' | kafka-console-producer.sh --broker-list master:9092 --topic highfreq_sensor"
    )
    time.sleep(1)
    stdin, stdout, stderr = ssh.exec_command(
        "kafka-console-consumer.sh --bootstrap-server master:9092 --topic highfreq_sensor --from-beginning --max-messages 1 --timeout-ms 5000 2>/dev/null"
    )
    result = stdout.read().decode().strip()
    if result:
        print(f"[OK] 手动发送成功: {result[:100]}")
    else:
        print("[FAIL] 手动发送失败")
    
    # 5. 重新启动Flink作业
    print("\n=== 5. 启动Flink作业 ===")
    
    # 先检查Flink是否运行
    stdin, stdout, stderr = ssh.exec_command("jps | grep StandaloneSession")
    flink_jobmanager = stdout.read().decode().strip()
    if not flink_jobmanager:
        print("Flink集群未运行，需要启动...")
        # 启动Flink
        ssh.exec_command("/opt/module/flink/bin/start-cluster.sh")
        time.sleep(5)
    
    # 检查现有作业
    stdin, stdout, stderr = ssh.exec_command("flink list 2>/dev/null")
    jobs = stdout.read().decode()
    print(f"当前作业:\n{jobs}")
    
    # 如果有运行中的作业，先取消
    if "RUNNING" in jobs:
        print("\n取消现有作业...")
        # 提取JobID并取消
        import re
        job_ids = re.findall(r'([a-f0-9]{32})', jobs)
        for job_id in job_ids:
            ssh.exec_command(f"flink cancel {job_id}")
            time.sleep(1)
    
    # 上传并提交新作业
    print("\n上传并提交Flink作业...")
    jar_local = "d:/Desktop/Match/MATCH/OverMatch/target/OverMatch-1.0-SNAPSHOT.jar"
    jar_remote = "/opt/module/flink/OverMatch-1.0-SNAPSHOT.jar"
    
    # 使用SFTP上传JAR
    transport = ssh.get_transport()
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        sftp.put(jar_local, jar_remote)
        print(f"[OK] JAR已上传到 {jar_remote}")
    except Exception as e:
        print(f"[WARN] 上传失败(可能已存在): {e}")
    sftp.close()
    
    # 提交作业
    cmd = f"flink run -d {jar_remote} 2>&1"
    print(f"执行: {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    result = stdout.read().decode()
    print(result)
    
    time.sleep(3)
    
    # 检查作业状态
    stdin, stdout, stderr = ssh.exec_command("flink list")
    print(f"\n作业状态:\n{stdout.read().decode()}")
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
