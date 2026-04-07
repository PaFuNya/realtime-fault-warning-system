#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查Flink日志找出启动失败原因
"""

import paramiko

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 检查Flink启动失败原因 ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 检查Flink日志目录
    print("\n=== 1. Flink日志文件 ===")
    stdin, stdout, stderr = ssh.exec_command("ls -lt /opt/module/flink/log/*.log | head -5")
    logs = stdout.read().decode().strip()
    print(logs)
    
    # 2. 检查JobManager日志
    print("\n=== 2. JobManager日志最后50行 ===")
    stdin, stdout, stderr = ssh.exec_command("ls -t /opt/module/flink/log/flink-*-standalonesession-*.log 2>/dev/null | head -1 | xargs tail -50")
    log_content = stdout.read().decode()
    print(log_content[-2000:] if len(log_content) > 2000 else log_content)
    
    # 3. 检查端口占用
    print("\n=== 3. 检查端口占用 ===")
    stdin, stdout, stderr = ssh.exec_command("netstat -tlnp | grep -E '8081|8082|8083|8084|6123' | head -10")
    ports = stdout.read().decode().strip()
    print(ports if ports else "无相关端口占用")
    
    # 4. 检查是否有僵尸进程
    print("\n=== 4. 检查Flink相关进程 ===")
    stdin, stdout, stderr = ssh.exec_command("ps aux | grep -E 'flink|Flink' | grep -v grep")
    processes = stdout.read().decode().strip()
    print(processes if processes else "无Flink进程")
    
    # 5. 手动启动JobManager看错误
    print("\n=== 5. 手动启动测试 ===")
    stdin, stdout, stderr = ssh.exec_command("pkill -9 -f StandaloneSession; pkill -9 -f TaskManager; sleep 2")
    
    # 使用不同的端口
    stdin, stdout, stderr = ssh.exec_command(
        "cd /opt/module/flink && bin/jobmanager.sh start-foreground 2>&1 &"
    )
    import time
    time.sleep(5)
    
    stdin, stdout, stderr = ssh.exec_command("jps | grep -i flink")
    flink_procs = stdout.read().decode().strip()
    print(f"Flink进程: {flink_procs}")
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
