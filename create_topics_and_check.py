#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创建Kafka topics并检查数据流
"""

import paramiko
import time

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 创建Kafka Topics并检查数据流 ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 检查Kafka是否运行
    print("\n=== 1. 检查Kafka状态 ===")
    stdin, stdout, stderr = ssh.exec_command("jps | grep Kafka")
    kafka = stdout.read().decode().strip()
    if kafka:
        print(f"[OK] Kafka运行中: {kafka}")
    else:
        print("[WARN] Kafka未运行，正在启动...")
        ssh.exec_command("/opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties")
        time.sleep(5)
    
    # 2. 创建topics
    print("\n=== 2. 创建Kafka Topics ===")
    topics = [
        ('highfreq_sensor', 3),
        ('device_state', 3),
        ('sensor_metrics', 3)
    ]
    for topic, partitions in topics:
        stdin, stdout, stderr = ssh.exec_command(
            f"/opt/module/kafka/bin/kafka-topics.sh --create --topic {topic} --bootstrap-server master:9092 --partitions {partitions} --replication-factor 1 2>&1"
        )
        result = stdout.read().decode().strip()
        print(f"  {topic}: {result}")
    
    # 3. 验证topics创建成功
    print("\n=== 3. 验证Topics ===")
    stdin, stdout, stderr = ssh.exec_command("/opt/module/kafka/bin/kafka-topics.sh --list --bootstrap-server master:9092")
    topics_list = stdout.read().decode().strip()
    print(f"现有topics: {topics_list}")
    
    # 4. 等待Flume发送数据
    print("\n=== 4. 等待Flume发送数据 (10秒) ===")
    time.sleep(10)
    
    # 5. 检查Kafka是否有数据
    print("\n=== 5. 检查Kafka数据 ===")
    stdin, stdout, stderr = ssh.exec_command(
        "/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic highfreq_sensor --from-beginning --max-messages 5 --timeout-ms 5000 2>/dev/null"
    )
    highfreq_data = stdout.read().decode().strip()
    if highfreq_data:
        print(f"[OK] highfreq_sensor 有数据 ({len(highfreq_data.split(chr(10)))} 条):")
        for line in highfreq_data.split('\n')[:3]:
            print(f"  {line[:100]}...")
    else:
        print("[WARN] highfreq_sensor 暂无数据")
        # 检查Flume日志
        print("\nFlume日志最后20行:")
        stdin, stdout, stderr = ssh.exec_command("tail -20 /var/log/flume-agent.log")
        print(stdout.read().decode())
    
    stdin, stdout, stderr = ssh.exec_command(
        "/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic device_state --from-beginning --max-messages 3 --timeout-ms 5000 2>/dev/null"
    )
    device_data = stdout.read().decode().strip()
    if device_data:
        print(f"\n[OK] device_state 有数据 ({len(device_data.split(chr(10)))} 条):")
        for line in device_data.split('\n')[:2]:
            print(f"  {line[:100]}...")
    else:
        print("\n[WARN] device_state 暂无数据")
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
