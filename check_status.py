#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查服务器状态：数据生成脚本、Flume、Kafka
"""

import paramiko

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 检查服务器状态 ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 检查数据生成脚本是否在运行
    print("\n=== 1. 数据生成脚本状态 ===")
    stdin, stdout, stderr = ssh.exec_command("ps aux | grep -E 'generate.*\.sh' | grep -v grep")
    scripts = stdout.read().decode().strip()
    if scripts:
        print("[OK] 数据生成脚本正在运行:")
        for line in scripts.split('\n'):
            print(f"  {line}")
    else:
        print("[WARN] 没有数据生成脚本在运行")
    
    # 2. 检查日志文件更新
    print("\n=== 2. 日志文件状态 ===")
    stdin, stdout, stderr = ssh.exec_command("tail -3 /data_log/highfreq_sensor_generated.log")
    highfreq_log = stdout.read().decode().strip()
    print(f"highfreq_sensor_generated.log 最后3行:")
    for line in highfreq_log.split('\n'):
        print(f"  {line[:120]}")
    
    stdin, stdout, stderr = ssh.exec_command("tail -1 /data_log/device_state_generated.log")
    device_log = stdout.read().decode().strip()
    print(f"\ndevice_state_generated.log 最后1行:")
    print(f"  {device_log[:120]}")
    
    # 3. 检查Flume状态
    print("\n=== 3. Flume状态 ===")
    stdin, stdout, stderr = ssh.exec_command("ps aux | grep flume | grep -v grep")
    flume = stdout.read().decode().strip()
    if flume:
        print("[OK] Flume正在运行:")
        print(f"  {flume}")
    else:
        print("[WARN] Flume未运行")
    
    # 4. 检查Kafka topics
    print("\n=== 4. Kafka Topics ===")
    stdin, stdout, stderr = ssh.exec_command("kafka-topics.sh --list --bootstrap-server master:9092 2>/dev/null")
    topics = stdout.read().decode().strip()
    print(f"存在的topics: {topics if topics else '无'}")
    
    # 5. 检查Kafka是否有数据
    print("\n=== 5. Kafka数据检查 ===")
    stdin, stdout, stderr = ssh.exec_command(
        "kafka-console-consumer.sh --bootstrap-server master:9092 --topic highfreq_sensor --from-beginning --max-messages 3 --timeout-ms 3000 2>/dev/null"
    )
    highfreq_data = stdout.read().decode().strip()
    if highfreq_data:
        print("[OK] highfreq_sensor topic 有数据:")
        for line in highfreq_data.split('\n')[:2]:
            print(f"  {line[:100]}...")
    else:
        print("[WARN] highfreq_sensor topic 暂无数据")
    
    stdin, stdout, stderr = ssh.exec_command(
        "kafka-console-consumer.sh --bootstrap-server master:9092 --topic device_state --from-beginning --max-messages 2 --timeout-ms 3000 2>/dev/null"
    )
    device_data = stdout.read().decode().strip()
    if device_data:
        print("\n[OK] device_state topic 有数据:")
        for line in device_data.split('\n')[:2]:
            print(f"  {line[:100]}...")
    else:
        print("\n[WARN] device_state topic 暂无数据")
    
    # 6. 检查Flink
    print("\n=== 6. Flink状态 ===")
    stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'StandaloneSession|TaskManager'")
    flink = stdout.read().decode().strip()
    if flink:
        print("[OK] Flink运行中:")
        for line in flink.split('\n'):
            print(f"  {line}")
    else:
        print("[WARN] Flink未运行")
    
    ssh.close()
    print("\n=== 检查完成 ===")

if __name__ == '__main__':
    main()
