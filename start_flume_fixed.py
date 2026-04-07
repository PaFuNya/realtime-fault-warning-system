#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
启动Flume agent，将日志数据发送到Kafka
"""

import paramiko
import time

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 连接到服务器启动Flume ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 检查Flume配置文件
    config_file = "/opt/module/flume/conf/MyFlume.conf"
    print(f"\n=== 1. 检查Flume配置文件 ===")
    stdin, stdout, stderr = ssh.exec_command(f"ls -la {config_file}")
    result = stdout.read().decode().strip()
    if result:
        print(f"找到配置文件: {result}")
        # 显示配置文件内容
        stdin, stdout, stderr = ssh.exec_command(f"cat {config_file}")
        print("\n配置文件内容:")
        print(stdout.read().decode())
    else:
        print(f"未找到配置文件: {config_file}")
        ssh.close()
        return
    
    # 2. 检查Flume安装
    flume_home = "/opt/module/flume"
    print(f"\n=== 2. Flume安装目录 ===")
    print(f"FLUME_HOME: {flume_home}")
    
    # 3. 检查是否有Flume进程在运行
    print("\n=== 3. 检查Flume进程 ===")
    stdin, stdout, stderr = ssh.exec_command("ps aux | grep flume | grep -v grep")
    flume_processes = stdout.read().decode().strip()
    if flume_processes:
        print(f"已有Flume进程在运行:\n{flume_processes}")
        print("\n先停止现有Flume进程...")
        ssh.exec_command("pkill -f flume-ng")
        time.sleep(2)
    else:
        print("没有Flume进程在运行")
    
    # 4. 启动Flume
    print("\n=== 4. 启动Flume Agent ===")
    cmd = f"cd {flume_home} && nohup bin/flume-ng agent --conf conf --conf-file {config_file} --name a1 -Dflume.root.logger=INFO,LOGFILE > /var/log/flume-agent.log 2>&1 &"
    print(f"执行命令: {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    time.sleep(3)
    
    # 检查是否启动成功
    stdin, stdout, stderr = ssh.exec_command("ps aux | grep flume-ng | grep -v grep | wc -l")
    count = int(stdout.read().decode().strip() or 0)
    if count > 0:
        print(f"[OK] Flume启动成功，{count}个进程在运行")
        
        # 显示日志
        print("\n=== 5. Flume启动日志 ===")
        time.sleep(2)
        stdin, stdout, stderr = ssh.exec_command("tail -30 /var/log/flume-agent.log 2>/dev/null")
        log = stdout.read().decode()
        print(log[-1000:] if len(log) > 1000 else log)
    else:
        print("[FAIL] Flume启动失败，查看日志...")
        stdin, stdout, stderr = ssh.exec_command("tail -50 /var/log/flume-agent.log 2>/dev/null")
        print(stdout.read().decode())
        ssh.close()
        return
    
    # 6. 等待并验证Kafka数据流
    print("\n=== 6. 验证Kafka数据流 (等待10秒) ===")
    time.sleep(10)
    
    # 检查highfreq_sensor topic
    stdin, stdout, stderr = ssh.exec_command(
        "kafka-console-consumer.sh --bootstrap-server master:9092 --topic highfreq_sensor --from-beginning --max-messages 3 --timeout-ms 5000 2>/dev/null"
    )
    result = stdout.read().decode().strip()
    if result:
        print(f"[OK] highfreq_sensor topic 有数据流入:")
        print(result[:300])
    else:
        print("[WARN] highfreq_sensor topic 暂无新数据")
    
    # 检查lowfreq_sensor topic
    stdin, stdout, stderr = ssh.exec_command(
        "kafka-console-consumer.sh --bootstrap-server master:9092 --topic lowfreq_sensor --from-beginning --max-messages 3 --timeout-ms 5000 2>/dev/null"
    )
    result = stdout.read().decode().strip()
    if result:
        print(f"\n[OK] lowfreq_sensor topic 有数据流入:")
        print(result[:300])
    else:
        print("\n[WARN] lowfreq_sensor topic 暂无新数据")
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
