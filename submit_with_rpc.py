#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用RPC地址提交Flink作业
"""

import paramiko
import time

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 使用RPC地址提交Flink作业 ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 检查Flink是否运行
    print("\n=== 1. 检查Flink状态 ===")
    stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'StandaloneSession|TaskManager'")
    processes = stdout.read().decode().strip()
    print(f"Flink进程:\n{processes}")
    
    # 2. 检查现有作业
    print("\n=== 2. 检查现有作业 ===")
    # 使用jobmanager的RPC地址
    stdin, stdout, stderr = ssh.exec_command(
        "export JOB_MANAGER_RPC_ADDRESS=master && /opt/module/flink/bin/flink list 2>&1"
    )
    jobs = stdout.read().decode()
    print(jobs)
    
    # 取消现有作业
    if "RUNNING" in jobs or "CREATED" in jobs:
        print("\n取消现有作业...")
        import re
        job_ids = re.findall(r'([a-f0-9]{32})', jobs)
        for job_id in job_ids:
            ssh.exec_command(f"export JOB_MANAGER_RPC_ADDRESS=master && /opt/module/flink/bin/flink cancel {job_id}")
            time.sleep(1)
    
    # 3. 提交作业
    print("\n=== 3. 提交Flink作业 ===")
    jar_file = "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
    
    # 使用-m参数指定JobManager地址
    cmd = f"/opt/module/flink/bin/flink run -m master:6123 -d {jar_file} 2>&1"
    print(f"执行: {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    result = stdout.read().decode()
    print(result)
    
    time.sleep(5)
    
    # 4. 检查作业状态
    print("\n=== 4. 检查作业状态 ===")
    stdin, stdout, stderr = ssh.exec_command(
        "/opt/module/flink/bin/flink list -m master:6123 2>&1"
    )
    job_status = stdout.read().decode()
    print(job_status)
    
    if "RUNNING" in job_status:
        print("\n[OK] Flink作业已成功启动并运行！")
        print("\n等待3分钟让高频窗口触发，检查var_temp和kurtosis_temp...")
        
        for i in range(3):
            time.sleep(60)
            print(f"  已等待 {i+1} 分钟...")
        
        # 检查窗口计算结果
        print("\n=== 5. 检查窗口计算结果 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "find /opt/module/flink/log -name '*.out' 2>/dev/null | head -1 | xargs grep -E 'var_temp|kurtosis_temp' 2>/dev/null | tail -5"
        )
        results = stdout.read().decode().strip()
        if results:
            print("[OK] 找到计算结果:")
            for line in results.split('\n'):
                print(f"  {line}")
        else:
            print("未找到var_temp/kurtosis_temp，检查Flink输出日志...")
            stdin, stdout, stderr = ssh.exec_command(
                "find /opt/module/flink/log -name '*.out' 2>/dev/null | head -1 | xargs tail -100 2>/dev/null"
            )
            output = stdout.read().decode()
            if output:
                lines = [l for l in output.split('\n') if '高频' in l or '窗口' in l or '特征' in l or '设备' in l]
                if lines:
                    print("Flink输出:")
                    for line in lines[-10:]:
                        print(f"  {line}")
                else:
                    print(output[-800:])
            else:
                print("未找到输出日志")
    else:
        print("\n[WARN] 作业未正常运行")
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
