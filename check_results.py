#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查Flink作业状态和窗口计算结果
"""

import paramiko
import time

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 检查Flink作业状态和结果 ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 检查Flink进程
    print("\n=== 1. Flink进程 ===")
    stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'StandaloneSession|TaskManager'")
    processes = stdout.read().decode().strip()
    print(processes if processes else "无Flink进程")
    
    # 2. 检查作业日志输出
    print("\n=== 2. Flink输出日志 ===")
    stdin, stdout, stderr = ssh.exec_command(
        "find /opt/module/flink/log -name '*.out' 2>/dev/null | head -1 | xargs tail -100 2>/dev/null"
    )
    output = stdout.read().decode()
    
    # 查找关键输出
    if output:
        lines = output.split('\n')
        
        # 查找高频窗口输出
        highfreq_lines = [l for l in lines if '高频' in l or '窗口' in l or '特征' in l]
        if highfreq_lines:
            print("高频窗口相关输出:")
            for line in highfreq_lines[-10:]:
                print(f"  {line}")
        
        # 查找var_temp和kurtosis_temp
        var_lines = [l for l in lines if 'var_temp' in l or 'kurtosis_temp' in l]
        if var_lines:
            print("\nvar_temp / kurtosis_temp 计算结果:")
            for line in var_lines[-5:]:
                print(f"  {line}")
        else:
            print("\n尚未找到var_temp/kurtosis_temp计算结果")
            print("等待3分钟后窗口触发...")
            
            # 等待并再次检查
            for i in range(3):
                time.sleep(60)
                print(f"  已等待 {i+1} 分钟...")
            
            # 重新检查
            stdin, stdout, stderr = ssh.exec_command(
                "find /opt/module/flink/log -name '*.out' 2>/dev/null | head -1 | xargs grep -E 'var_temp|kurtosis_temp' 2>/dev/null | tail -5"
            )
            results = stdout.read().decode().strip()
            if results:
                print("\n[OK] 找到计算结果:")
                for line in results.split('\n'):
                    print(f"  {line}")
            else:
                print("\n仍未找到结果，检查完整日志...")
                stdin, stdout, stderr = ssh.exec_command(
                    "find /opt/module/flink/log -name '*.out' 2>/dev/null | head -1 | xargs tail -50 2>/dev/null"
                )
                print(stdout.read().decode()[-800:])
    else:
        print("未找到输出日志")
    
    # 3. 检查Kafka warning_log topic是否有报警数据输出
    print("\n=== 3. 检查Kafka warning_log输出 ===")
    stdin, stdout, stderr = ssh.exec_command(
        "kafka-console-consumer.sh --bootstrap-server master:9092 --topic warning_log --from-beginning --max-messages 3 --timeout-ms 5000 2>/dev/null"
    )
    warnings = stdout.read().decode().strip()
    if warnings:
        print("[OK] warning_log 有数据:")
        for line in warnings.split('\n')[:2]:
            print(f"  {line[:150]}...")
    else:
        print("warning_log 暂无数据（可能还没有报警触发）")
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
