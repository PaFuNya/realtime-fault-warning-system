#!/usr/bin/env python3
import paramiko
import sys
import json

def analyze_variance():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        # 分析高频数据中的温度变化
        print("=== 分析高频数据中的温度变化 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cat /data_log/highfreq_sensor_generated.log | head -100"
        )
        logs = stdout.read().decode().strip()
        
        # 按设备分组统计温度
        temps_by_machine = {}
        for line in logs.split('\n'):
            try:
                data = json.loads(line)
                mid = data['machineId']
                temp = data['temperature']
                if mid not in temps_by_machine:
                    temps_by_machine[mid] = []
                temps_by_machine[mid].append(temp)
            except:
                pass
        
        print("各设备温度统计（前100条数据）:")
        for mid in sorted(temps_by_machine.keys()):
            temps = temps_by_machine[mid]
            if len(temps) > 1:
                mean = sum(temps) / len(temps)
                variance = sum((t - mean) ** 2 for t in temps) / len(temps)
                print(f"  设备{mid}: 样本数={len(temps)}, 均值={mean:.2f}, 方差={variance:.4f}, 最小={min(temps):.1f}, 最大={max(temps):.1f}")
        
        # 检查Flink代码中的方差计算
        print("\n=== 问题分析 ===")
        print("var_temp=0.0 可能的原因:")
        print("1. 温度值变化太小（所有值几乎相同）")
        print("2. 窗口内数据点太少（n<2）")
        print("3. 计算逻辑问题")
        
        # 查看原始数据样例
        print("\n=== 原始数据样例 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "grep 'machineId.*109' /data_log/highfreq_sensor_generated.log | head -5"
        )
        sample = stdout.read().decode().strip()
        print("设备109的5条数据:")
        for line in sample.split('\n'):
            try:
                data = json.loads(line)
                print(f"  时间:{data['timestamp']} 温度:{data['temperature']}")
            except:
                pass
        
        ssh.close()
        print("\n=== 分析完成 ===")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    analyze_variance()
