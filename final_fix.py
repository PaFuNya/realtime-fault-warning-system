#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终修复所有问题
"""

import paramiko
import time

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 最终修复 ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 检查Kafka是否运行
    print("\n=== 1. 检查Kafka状态 ===")
    stdin, stdout, stderr = ssh.exec_command("jps | grep Kafka")
    kafka_process = stdout.read().decode().strip()
    if kafka_process:
        print(f"[OK] Kafka运行中: {kafka_process}")
    else:
        print("[WARN] Kafka未运行，需要启动")
        ssh.exec_command("/opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties")
        time.sleep(5)
    
    # 2. 创建Kafka topics
    print("\n=== 2. 创建Kafka topics ===")
    topics = ['highfreq_sensor', 'device_state', 'sensor_metrics']
    for topic in topics:
        stdin, stdout, stderr = ssh.exec_command(
            f"/opt/module/kafka/bin/kafka-topics.sh --create --topic {topic} --bootstrap-server master:9092 --partitions 3 --replication-factor 1 2>&1"
        )
        result = stdout.read().decode().strip()
        if "Created" in result or "exists" in result:
            print(f"[OK] {topic}: {result}")
        else:
            print(f"[INFO] {topic}: {result}")
    
    # 3. 测试Kafka
    print("\n=== 3. 测试Kafka发送 ===")
    test_msg = '{"machineId":109,"timestamp":"2026-04-07 11:30:00","temperature":45.0}'
    stdin, stdout, stderr = ssh.exec_command(
        f"echo '{test_msg}' | /opt/module/kafka/bin/kafka-console-producer.sh --broker-list master:9092 --topic highfreq_sensor"
    )
    time.sleep(1)
    stdin, stdout, stderr = ssh.exec_command(
        "/opt/module/kafka/bin/kafka-console-consumer.sh --bootstrap-server master:9092 --topic highfreq_sensor --from-beginning --max-messages 1 --timeout-ms 5000 2>&1"
    )
    result = stdout.read().decode().strip()
    if result and "machineId" in result:
        print(f"[OK] Kafka发送成功: {result[:100]}")
    else:
        print(f"[WARN] Kafka测试: {result}")
    
    # 4. 重启Flume（因为topic现在存在了）
    print("\n=== 4. 重启Flume ===")
    ssh.exec_command("pkill -f flume-ng")
    time.sleep(2)
    cmd = "cd /opt/module/flume && nohup bin/flume-ng agent --conf conf --conf-file conf/MyFlume.conf --name a1 -Dflume.root.logger=INFO,LOGFILE > /var/log/flume-agent.log 2>&1 &"
    ssh.exec_command(cmd)
    time.sleep(3)
    
    stdin, stdout, stderr = ssh.exec_command("ps aux | grep flume-ng | grep -v grep | wc -l")
    count = int(stdout.read().decode().strip() or 0)
    print(f"[OK] Flume重启成功，{count}个进程" if count > 0 else "[FAIL] Flume启动失败")
    
    # 5. 启动Flink集群
    print("\n=== 5. 启动Flink集群 ===")
    
    # 检查Flink是否运行
    stdin, stdout, stderr = ssh.exec_command("jps | grep StandaloneSession")
    if not stdout.read().decode().strip():
        print("启动Flink集群...")
        ssh.exec_command("/opt/module/flink/bin/start-cluster.sh")
        time.sleep(5)
    
    # 修改配置避免端口冲突
    print("修改Flink配置使用8083端口...")
    ssh.exec_command("sed -i 's/#rest.port: 8081/rest.port: 8083/' /opt/module/flink/conf/flink-conf.yaml")
    ssh.exec_command("sed -i 's/rest.port: 8081/rest.port: 8083/' /opt/module/flink/conf/flink-conf.yaml")
    ssh.exec_command("sed -i 's/rest.port: 8082/rest.port: 8083/' /opt/module/flink/conf/flink-conf.yaml")
    
    # 重启Flink
    ssh.exec_command("/opt/module/flink/bin/stop-cluster.sh")
    time.sleep(2)
    ssh.exec_command("/opt/module/flink/bin/start-cluster.sh")
    time.sleep(5)
    
    stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'StandaloneSession|TaskManager'")
    flink_processes = stdout.read().decode().strip()
    print(f"Flink进程:\n{flink_processes}")
    
    # 6. 编译并上传JAR
    print("\n=== 6. 编译Flink项目 ===")
    # 这里需要在本地编译
    print("请在本地执行: mvn clean package -DskipTests")
    print("然后上传JAR到服务器")
    
    # 检查服务器上是否已有JAR
    stdin, stdout, stderr = ssh.exec_command("ls -la /opt/module/flink/*.jar 2>/dev/null | grep -v slf4j")
    existing_jars = stdout.read().decode().strip()
    if existing_jars:
        print(f"服务器上已有JAR:\n{existing_jars}")
        jar_file = "/opt/module/flink/OverMatch-1.0-SNAPSHOT.jar"
    else:
        print("服务器上没有JAR文件，需要上传")
        jar_file = None
    
    # 7. 提交Flink作业
    if jar_file:
        print(f"\n=== 7. 提交Flink作业 ===")
        # 取消现有作业
        stdin, stdout, stderr = ssh.exec_command("/opt/module/flink/bin/flink list 2>/dev/null")
        jobs = stdout.read().decode()
        if "RUNNING" in jobs:
            import re
            job_ids = re.findall(r'([a-f0-9]{32})', jobs)
            for job_id in job_ids:
                ssh.exec_command(f"/opt/module/flink/bin/flink cancel {job_id}")
                time.sleep(1)
        
        # 提交新作业
        cmd = f"/opt/module/flink/bin/flink run -d {jar_file} 2>&1"
        print(f"执行: {cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd)
        result = stdout.read().decode()
        print(result)
        
        time.sleep(3)
        stdin, stdout, stderr = ssh.exec_command("/opt/module/flink/bin/flink list")
        print(f"\n作业状态:\n{stdout.read().decode()}")
    
    ssh.close()
    print("\n=== 完成 ===")
    print("\n下一步:")
    print("1. 在本地编译项目: mvn clean package -DskipTests")
    print("2. 上传JAR到服务器")
    print("3. 重新运行此脚本提交作业")

if __name__ == '__main__':
    main()
