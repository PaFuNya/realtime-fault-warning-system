#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直接在服务器上后台启动Flink作业
"""

import paramiko
import time

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 后台启动Flink作业 ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 清理并启动Flink
    print("\n=== 1. 启动Flink集群 ===")
    ssh.exec_command("pkill -9 -f StandaloneSession; pkill -9 -f TaskManager")
    time.sleep(2)
    
    # 使用默认配置启动
    stdin, stdout, stderr = ssh.exec_command("cd /opt/module/flink && bin/start-cluster.sh 2>&1")
    print(stdout.read().decode())
    time.sleep(5)
    
    # 2. 检查Flink进程
    print("\n=== 2. 检查Flink进程 ===")
    stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'StandaloneSession|TaskManager'")
    processes = stdout.read().decode().strip()
    print(processes)
    
    if not processes:
        print("[FAIL] Flink未启动")
        ssh.close()
        return
    
    # 3. 直接运行JAR（不使用flink run命令，避免REST连接问题）
    print("\n=== 3. 启动Flink作业 ===")
    jar_file = "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
    
    # 创建启动脚本
    start_script = f"""#!/bin/bash
cd /opt/module/flink
nohup java -cp "lib/*:lib/flink-dist_2.12-1.14.6.jar:$jar_file" org.example.tasks.FlinkRulInference > /var/log/flink-job.log 2>&1 &
echo $! > /tmp/flink-job.pid
"""
    
    # 使用flink run但捕获输出
    cmd = f"cd /opt/module/flink && nohup bin/flink run -d {jar_file} > /var/log/flink-submit.log 2>&1 &"
    print(f"执行: {cmd}")
    ssh.exec_command(cmd)
    
    time.sleep(10)
    
    # 4. 检查作业是否运行
    print("\n=== 4. 检查作业状态 ===")
    stdin, stdout, stderr = ssh.exec_command("ps aux | grep FlinkRulInference | grep -v grep")
    job_process = stdout.read().decode().strip()
    if job_process:
        print(f"[OK] 作业进程:\n{job_process}")
    else:
        print("[WARN] 未找到作业进程，检查提交日志...")
        stdin, stdout, stderr = ssh.exec_command("tail -50 /var/log/flink-submit.log")
        print(stdout.read().decode())
    
    # 5. 检查日志输出
    print("\n=== 5. 检查作业日志 ===")
    stdin, stdout, stderr = ssh.exec_command("tail -100 /var/log/flink-submit.log")
    log = stdout.read().decode()
    if log:
        print(log[-1500:])
    
    # 6. 等待并检查结果
    print("\n=== 6. 等待窗口触发（3分钟） ===")
    for i in range(3):
        time.sleep(60)
        print(f"  已等待 {i+1} 分钟...")
    
    print("\n=== 7. 检查计算结果 ===")
    stdin, stdout, stderr = ssh.exec_command(
        "grep -E 'var_temp|kurtosis_temp' /var/log/flink-submit.log | tail -5"
    )
    results = stdout.read().decode().strip()
    if results:
        print("[OK] 找到计算结果:")
        for line in results.split('\n'):
            print(f"  {line}")
    else:
        print("未找到var_temp/kurtosis_temp，检查完整日志...")
        stdin, stdout, stderr = ssh.exec_command("tail -80 /var/log/flink-submit.log")
        print(stdout.read().decode()[-1000:])
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
