#!/usr/bin/env python3
import paramiko
import sys
import os

def fix_server():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        # 查找ETL目录
        print("=== 1. 查找ETL脚本目录 ===")
        stdin, stdout, stderr = ssh.exec_command("find / -name 'generate_change_data.sh' 2>/dev/null | head -1")
        etl_path = stdout.read().decode().strip()
        if etl_path:
            etl_dir = os.path.dirname(etl_path)
            print(f"找到ETL目录: {etl_dir}")
        else:
            print("未找到ETL脚本，需要上传")
            etl_dir = "/root/ETL"
        
        # 检查高频脚本是否存在
        print("\n=== 2. 检查高频数据生成脚本 ===")
        stdin, stdout, stderr = ssh.exec_command(f"ls -la {etl_dir}/generate_highfreq_sensor.sh 2>/dev/null")
        if "No such file" in stdout.read().decode():
            print("高频脚本不存在，需要上传")
        else:
            print(f"高频脚本存在: {etl_dir}/generate_highfreq_sensor.sh")
        
        # 启动低频数据生成
        print("\n=== 3. 启动低频数据生成脚本 ===")
        stdin, stdout, stderr = ssh.exec_command(f"cd {etl_dir} && nohup bash generate_change_data.sh > /dev/null 2>&1 &")
        print("低频脚本已启动")
        
        # 启动高频数据生成
        print("\n=== 4. 启动高频数据生成脚本 ===")
        stdin, stdout, stderr = ssh.exec_command(f"cd {etl_dir} && nohup bash generate_highfreq_sensor.sh 1 > /dev/null 2>&1 &")
        print("高频脚本已启动")
        
        # 等待几秒让脚本启动
        import time
        time.sleep(2)
        
        # 验证脚本是否运行
        print("\n=== 5. 验证脚本是否运行 ===")
        stdin, stdout, stderr = ssh.exec_command("ps aux | grep generate | grep -v grep")
        result = stdout.read().decode().strip()
        print(result if result else "警告: 脚本可能未启动")
        
        # 检查高频日志是否开始生成
        print("\n=== 6. 检查高频日志 ===")
        stdin, stdout, stderr = ssh.exec_command("tail -3 /data_log/highfreq_sensor_generated.log 2>/dev/null || echo '日志尚未生成'")
        print(stdout.read().decode().strip())
        
        # 检查Kafka
        print("\n=== 7. 检查Kafka状态 ===")
        stdin, stdout, stderr = ssh.exec_command("jps | grep -E '(Kafka|Quorum)'")
        kafka_result = stdout.read().decode().strip()
        print(f"Kafka进程: {kafka_result if kafka_result else '未运行'}")
        
        # 检查Flink
        print("\n=== 8. 检查Flink状态 ===")
        stdin, stdout, stderr = ssh.exec_command("jps | grep -E '(Standalone|TaskManager)'")
        flink_result = stdout.read().decode().strip()
        print(f"Flink进程: {flink_result if flink_result else '未运行'}")
        
        ssh.close()
        print("\n=== 修复完成 ===")
        print("\n下一步:")
        print("1. 等待3分钟让高频窗口触发")
        print("2. 启动Flink作业: flink run -c org.example.tasks.FlinkRulInference your-jar.jar")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    fix_server()
