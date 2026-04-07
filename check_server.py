#!/usr/bin/env python3
import paramiko
import sys

def check_server():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        # 检查进程
        print("=== 1. 检查运行中的数据生成脚本 ===")
        stdin, stdout, stderr = ssh.exec_command("ps aux | grep -E 'generate' | grep -v grep")
        result = stdout.read().decode().strip()
        print(result if result else "没有找到数据生成脚本")
        
        # 检查Flink
        print("\n=== 2. 检查Flink作业 ===")
        stdin, stdout, stderr = ssh.exec_command("flink list 2>/dev/null || echo 'Flink未运行或命令不可用'")
        print(stdout.read().decode().strip())
        
        # 检查日志文件
        print("\n=== 3. 检查日志文件 ===")
        stdin, stdout, stderr = ssh.exec_command("ls -la ~/data_log/*.log 2>/dev/null || ls -la /data_log/*.log 2>/dev/null || echo '在查找日志文件...'")
        print(stdout.read().decode().strip())
        
        # 检查高频日志最后几行
        print("\n=== 4. 高频数据日志最后3行 ===")
        stdin, stdout, stderr = ssh.exec_command("tail -3 ~/data_log/highfreq_sensor_generated.log 2>/dev/null || tail -3 /data_log/highfreq_sensor_generated.log 2>/dev/null || echo '未找到日志文件'")
        print(stdout.read().decode().strip())
        
        # 检查Kafka topics
        print("\n=== 5. 检查Kafka Topics ===")
        stdin, stdout, stderr = ssh.exec_command("kafka-topics.sh --list --bootstrap-server master:9092 2>/dev/null || echo 'Kafka命令不可用'")
        print(stdout.read().decode().strip())
        
        ssh.close()
        print("\n=== 检查完成 ===")
        
    except Exception as e:
        print(f"连接失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    check_server()
