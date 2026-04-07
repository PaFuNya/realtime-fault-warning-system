#!/usr/bin/env python3
import paramiko
import sys
import time

def fix_flink():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        # 检查当前Java进程
        print("=== 1. 检查当前Java进程 ===")
        stdin, stdout, stderr = ssh.exec_command("jps")
        print(stdout.read().decode().strip())
        
        # 停止所有Flink进程
        print("\n=== 2. 停止所有Flink进程 ===")
        stdin, stdout, stderr = ssh.exec_command("pkill -f flink; pkill -f TaskManager; pkill -f Standalone")
        time.sleep(2)
        print("已停止")
        
        # 清理Flink临时文件
        print("\n=== 3. 清理Flink临时文件 ===")
        stdin, stdout, stderr = ssh.exec_command("rm -rf /tmp/flink-* /opt/module/flink/log/*.log /opt/module/flink/log/*.out")
        print("已清理")
        
        # 启动Flink集群
        print("\n=== 4. 启动Flink集群 ===")
        stdin, stdout, stderr = ssh.exec_command("cd /opt/module/flink && bin/start-cluster.sh")
        time.sleep(5)
        
        # 检查Flink是否启动
        print("\n=== 5. 验证Flink启动 ===")
        stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'Standalone|TaskManager'")
        result = stdout.read().decode().strip()
        print(result if result else "Flink未启动")
        
        if not result:
            print("\n尝试查看启动日志...")
            stdin, stdout, stderr = ssh.exec_command("cat /opt/module/flink/log/flink-*.log 2>/dev/null | tail -20")
            print(stdout.read().decode().strip())
        
        # 提交Flink作业
        print("\n=== 6. 提交Flink作业 ===")
        flink_cmd = (
            "cd /opt/module/flink && "
            "bin/flink run -d "
            "-c org.example.tasks.FlinkRulInference "
            "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
        )
        stdin, stdout, stderr = ssh.exec_command(flink_cmd)
        result = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        
        print("输出:", result)
        if error:
            print("错误:", error)
        
        # 等待作业启动
        time.sleep(5)
        
        # 检查作业状态
        print("\n=== 7. 检查作业状态 ===")
        stdin, stdout, stderr = ssh.exec_command("cd /opt/module/flink && bin/flink list")
        print(stdout.read().decode().strip())
        
        ssh.close()
        print("\n=== 修复完成 ===")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    fix_flink()
