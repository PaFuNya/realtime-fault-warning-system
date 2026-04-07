#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直接在服务器上执行Flink命令
"""

import paramiko
import time

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 直接提交Flink作业 ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 先停止所有Flink进程，清理环境
    print("\n=== 1. 清理Flink环境 ===")
    ssh.exec_command("pkill -9 -f StandaloneSession")
    ssh.exec_command("pkill -9 -f TaskManager")
    time.sleep(3)
    
    # 2. 修改配置使用可用端口
    print("\n=== 2. 修改Flink配置 ===")
    # 找一个可用的端口
    stdin, stdout, stderr = ssh.exec_command("netstat -tln | grep -E '808[0-9]' | awk '{print $4}' | cut -d: -f2 | sort -n | tail -1")
    last_port = stdout.read().decode().strip()
    new_port = int(last_port) + 1 if last_port else 8084
    print(f"使用端口: {new_port}")
    
    # 修改配置
    ssh.exec_command(f"echo 'rest.port: {new_port}' >> /opt/module/flink/conf/flink-conf.yaml")
    ssh.exec_command(f"echo 'rest.address: master' >> /opt/module/flink/conf/flink-conf.yaml")
    
    # 3. 启动Flink
    print("\n=== 3. 启动Flink集群 ===")
    stdin, stdout, stderr = ssh.exec_command("cd /opt/module/flink && bin/start-cluster.sh 2>&1")
    output = stdout.read().decode()
    print(output)
    time.sleep(5)
    
    # 检查进程
    stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'StandaloneSession|TaskManager'")
    processes = stdout.read().decode().strip()
    print(f"Flink进程:\n{processes}")
    
    # 4. 等待REST服务
    print(f"\n=== 4. 等待REST服务就绪 (端口{new_port}) ===")
    for i in range(15):
        stdin, stdout, stderr = ssh.exec_command(f"curl -s http://master:{new_port}/overview 2>/dev/null | head -1")
        response = stdout.read().decode()
        if response and "version" in response:
            print(f"[OK] REST服务就绪")
            break
        time.sleep(2)
    else:
        print("[WARN] REST服务未就绪，但继续尝试提交")
    
    # 5. 提交作业
    print("\n=== 5. 提交Flink作业 ===")
    jar_file = "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
    
    # 创建提交脚本
    submit_script = f"""#!/bin/bash
export FLINK_CONF_DIR=/opt/module/flink/conf
cd /opt/module/flink
./bin/flink run -m master:{new_port} -d {jar_file}
"""
    stdin, stdout, stderr = ssh.exec_command(f"cat > /tmp/submit_flink.sh << 'EOF'\n{submit_script}\nEOF")
    ssh.exec_command("chmod +x /tmp/submit_flink.sh")
    
    # 执行提交
    stdin, stdout, stderr = ssh.exec_command("bash /tmp/submit_flink.sh 2>&1")
    result = stdout.read().decode()
    print(result)
    
    time.sleep(5)
    
    # 6. 检查作业状态
    print("\n=== 6. 检查作业状态 ===")
    stdin, stdout, stderr = ssh.exec_command(f"/opt/module/flink/bin/flink list -m master:{new_port} 2>&1")
    job_status = stdout.read().decode()
    print(job_status)
    
    if "RUNNING" in job_status:
        print("\n[OK] Flink作业已成功启动！")
        print("\n等待3分钟让高频窗口触发...")
        for i in range(3):
            time.sleep(60)
            print(f"  已等待 {i+1} 分钟...")
        
        # 检查窗口计算结果
        print("\n=== 7. 检查窗口计算结果 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "find /opt/module/flink/log -name '*.out' 2>/dev/null | head -1 | xargs grep -E 'var_temp|kurtosis_temp' 2>/dev/null | tail -5"
        )
        results = stdout.read().decode().strip()
        if results:
            print("[OK] 找到计算结果:")
            for line in results.split('\n'):
                print(f"  {line}")
        else:
            print("未找到var_temp/kurtosis_temp，检查日志...")
            stdin, stdout, stderr = ssh.exec_command(
                "find /opt/module/flink/log -name '*.out' 2>/dev/null | head -1 | xargs tail -50 2>/dev/null"
            )
            print(stdout.read().decode()[-500:])
    else:
        print("\n[WARN] 作业未正常运行")
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
