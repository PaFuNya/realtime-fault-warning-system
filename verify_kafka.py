#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证Kafka数据流是否正常
"""

import paramiko
import time

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 验证Kafka数据流 ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 等待Flume收集一些数据
    print("\n等待10秒让Flume收集数据...")
    time.sleep(10)
    
    # 1. 检查highfreq_sensor topic
    print("\n=== 1. 检查 highfreq_sensor topic ===")
    stdin, stdout, stderr = ssh.exec_command(
        "kafka-console-consumer.sh --bootstrap-server master:9092 --topic highfreq_sensor --from-beginning --max-messages 5 --timeout-ms 10000 2>/dev/null"
    )
    result = stdout.read().decode().strip()
    if result:
        print(f"[OK] highfreq_sensor 有数据:")
        for line in result.split('\n')[:3]:
            print(f"  {line[:150]}...")
    else:
        print("[WARN] highfreq_sensor 暂无数据")
        # 检查日志文件是否存在
        stdin, stdout, stderr = ssh.exec_command("ls -la /data_log/highfreq_sensor_generated.log")
        print(f"日志文件状态:\n{stdout.read().decode()}")
    
    # 2. 检查device_state topic
    print("\n=== 2. 检查 device_state topic ===")
    stdin, stdout, stderr = ssh.exec_command(
        "kafka-console-consumer.sh --bootstrap-server master:9092 --topic device_state --from-beginning --max-messages 3 --timeout-ms 10000 2>/dev/null"
    )
    result = stdout.read().decode().strip()
    if result:
        print(f"[OK] device_state 有数据:")
        for line in result.split('\n')[:2]:
            print(f"  {line[:150]}...")
    else:
        print("[WARN] device_state 暂无数据")
    
    # 3. 检查Flink作业状态
    print("\n=== 3. 检查Flink作业状态 ===")
    stdin, stdout, stderr = ssh.exec_command("flink list 2>/dev/null || echo 'Flink未运行'")
    result = stdout.read().decode().strip()
    print(result)
    
    # 4. 如果Flink在运行，检查窗口计算结果
    if "RUNNING" in result:
        print("\n=== 4. 检查Flink窗口计算结果 ===")
        # 等待3分钟让窗口触发
        print("等待3分钟让高频窗口触发...")
        time.sleep(180)
        
        # 检查Flink日志中的窗口输出
        stdin, stdout, stderr = ssh.exec_command(
            "find /opt/module/flink/log -name '*.out' -exec grep -l 'var_temp' {} \\; 2>/dev/null | head -1"
        )
        log_file = stdout.read().decode().strip()
        if log_file:
            stdin, stdout, stderr = ssh.exec_command(f"grep 'var_temp' {log_file} | tail -5")
            var_results = stdout.read().decode().strip()
            if var_results:
                print(f"[OK] 找到 var_temp 计算结果:")
                for line in var_results.split('\n')[-3:]:
                    print(f"  {line}")
            else:
                print("[WARN] 尚未找到 var_temp 计算结果")
        else:
            print("[WARN] 未找到Flink输出日志文件")
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
