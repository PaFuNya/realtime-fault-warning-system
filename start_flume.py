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
    
    # 1. 检查Flume配置文件是否存在
    print("\n=== 1. 检查Flume配置文件 ===")
    stdin, stdout, stderr = ssh.exec_command("find / -name 'myflume.conf' 2>/dev/null")
    config_file = stdout.read().decode().strip()
    if config_file:
        print(f"找到配置文件: {config_file}")
    else:
        print("未找到myflume.conf，检查其他Flume配置...")
        stdin, stdout, stderr = ssh.exec_command("find / -name '*.conf' -path '*flume*' 2>/dev/null | head -5")
        print(stdout.read().decode())
    
    # 2. 检查Flume是否已安装
    print("\n=== 2. 检查Flume安装 ===")
    stdin, stdout, stderr = ssh.exec_command("which flume-ng || find / -name 'flume-ng' -type f 2>/dev/null | head -1")
    flume_path = stdout.read().decode().strip()
    if flume_path:
        print(f"Flume路径: {flume_path}")
    else:
        print("未找到Flume，检查常见安装目录...")
        stdin, stdout, stderr = ssh.exec_command("ls -la /opt/flume*/bin/flume-ng 2>/dev/null || ls -la /usr/local/flume*/bin/flume-ng 2>/dev/null || ls -la ~/flume*/bin/flume-ng 2>/dev/null")
        print(stdout.read().decode())
    
    # 3. 检查是否有Flume进程在运行
    print("\n=== 3. 检查Flume进程 ===")
    stdin, stdout, stderr = ssh.exec_command("ps aux | grep flume | grep -v grep")
    flume_processes = stdout.read().decode().strip()
    if flume_processes:
        print(f"已有Flume进程在运行:\n{flume_processes}")
    else:
        print("没有Flume进程在运行")
    
    # 4. 启动Flume（如果配置文件存在且Flume未运行）
    if config_file and not flume_processes:
        print("\n=== 4. 启动Flume Agent ===")
        # 确定Flume安装目录
        stdin, stdout, stderr = ssh.exec_command("dirname $(dirname $(find /opt -name 'flume-ng' -type f 2>/dev/null | head -1)) 2>/dev/null || echo '/opt/flume'")
        flume_home = stdout.read().decode().strip() or '/opt/flume'
        
        # 在后台启动Flume
        cmd = f"cd {flume_home} && nohup bin/flume-ng agent --conf conf --conf-file {config_file} --name a1 -Dflume.root.logger=INFO,console > /var/log/flume-agent.log 2>&1 &"
        print(f"执行命令: {cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd)
        time.sleep(2)
        
        # 检查是否启动成功
        stdin, stdout, stderr = ssh.exec_command("ps aux | grep flume | grep -v grep | wc -l")
        count = int(stdout.read().decode().strip() or 0)
        if count > 0:
            print(f"[OK] Flume启动成功，{count}个进程在运行")
        else:
            print("[FAIL] Flume启动失败，查看日志...")
            stdin, stdout, stderr = ssh.exec_command("tail -20 /var/log/flume-agent.log 2>/dev/null || echo '无日志文件'")
            print(stdout.read().decode())
    
    # 5. 验证Kafka topic是否有数据流入
    print("\n=== 5. 验证Kafka数据流 ===")
    time.sleep(3)
    stdin, stdout, stderr = ssh.exec_command(
        "kafka-console-consumer.sh --bootstrap-server master:9092 --topic highfreq_sensor --from-beginning --max-messages 3 --timeout-ms 5000 2>/dev/null || echo '暂无数据'"
    )
    result = stdout.read().decode().strip()
    if result and result != '暂无数据':
        print(f"[OK] highfreq_sensor topic 有数据流入:\n{result[:200]}...")
    else:
        print("[WARN] highfreq_sensor topic 暂无新数据")
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
