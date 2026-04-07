#!/usr/bin/env python3
import paramiko
import sys
import time
from scp import SCPClient

def update_and_restart():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        # 停止当前数据生成脚本
        print("=== 1. 停止当前数据生成脚本 ===")
        ssh.exec_command("pkill -f generate_highfreq_sensor")
        ssh.exec_command("pkill -f generate_change_data")
        time.sleep(2)
        print("已停止")
        
        # 备份旧日志
        print("\n=== 2. 备份旧日志 ===")
        ssh.exec_command("mv /data_log/highfreq_sensor_generated.log /data_log/highfreq_sensor_generated.log.old 2>/dev/null")
        ssh.exec_command("mv /data_log/device_state_generated.log /data_log/device_state_generated.log.old 2>/dev/null")
        ssh.exec_command("mv /data_log/sensor_metrics_generated.log /data_log/sensor_metrics_generated.log.old 2>/dev/null")
        print("已备份")
        
        # 上传修复后的脚本
        print("\n=== 3. 上传修复后的高频数据生成脚本 ===")
        local_script = "d:\\Desktop\\Match\\MATCH\\OverMatch\\ETL\\generate_highfreq_sensor.sh"
        remote_script = "/data_log/generate_highfreq_sensor.sh"
        
        with SCPClient(ssh.get_transport()) as scp:
            scp.put(local_script, remote_script)
        print("上传完成")
        
        # 设置权限
        ssh.exec_command(f"chmod +x {remote_script}")
        
        # 启动低频数据生成
        print("\n=== 4. 启动低频数据生成 ===")
        ssh.exec_command("cd /data_log && nohup bash generate_change_data.sh > /dev/null 2>&1 &")
        print("已启动 generate_change_data.sh")
        
        # 启动高频数据生成
        print("\n=== 5. 启动高频数据生成 ===")
        ssh.exec_command("cd /data_log && nohup bash generate_highfreq_sensor.sh 1 > /dev/null 2>&1 &")
        print("已启动 generate_highfreq_sensor.sh")
        
        time.sleep(3)
        
        # 验证启动
        print("\n=== 6. 验证脚本运行 ===")
        stdin, stdout, stderr = ssh.exec_command("ps aux | grep generate | grep -v grep")
        result = stdout.read().decode().strip()
        print(result)
        
        # 检查新日志
        print("\n=== 7. 检查新日志前5行 ===")
        time.sleep(5)  # 等待生成一些数据
        stdin, stdout, stderr = ssh.exec_command("head -5 /data_log/highfreq_sensor_generated.log 2>/dev/null")
        logs = stdout.read().decode().strip()
        print(logs)
        
        # 分析温度变化
        print("\n=== 8. 验证温度是否有变化 ===")
        stdin, stdout, stderr = ssh.exec_command(
            'cat /data_log/highfreq_sensor_generated.log | head -50 | grep -o \'temperature\":[0-9.]*\' | sort -u'
        )
        temps = stdout.read().decode().strip()
        temp_count = len(temps.split('\n')) if temps else 0
        print(f"前50条数据中有 {temp_count} 个不同的温度值")
        if temp_count > 5:
            print("[OK] 温度有变化，方差计算应该正常")
        else:
            print("[WARN] 温度变化仍然太小")
        
        ssh.close()
        print("\n=== 完成 ===")
        print("\n请等待3分钟后，高频窗口将触发，var_temp 和 kurtosis_temp 应该能正常计算")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    update_and_restart()
