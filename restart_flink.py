#!/usr/bin/env python3
import paramiko
import sys
import time

def restart_flink():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        # 强制停止所有Flink相关进程
        print("=== 1. 强制停止所有Flink进程 ===")
        ssh.exec_command("pkill -9 -f flink 2>/dev/null")
        ssh.exec_command("pkill -9 -f StandaloneSession 2>/dev/null")
        ssh.exec_command("pkill -9 -f TaskManagerRunner 2>/dev/null")
        time.sleep(3)
        
        # 在slave节点上也停止
        for slave in ['slave1', 'slave2']:
            ssh.exec_command(f"ssh {slave} 'pkill -9 -f TaskManagerRunner 2>/dev/null'")
        time.sleep(2)
        
        # 检查进程
        stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'Standalone|TaskManager'")
        print("剩余进程:", stdout.read().decode().strip() or "无")
        
        # 清理临时文件
        print("\n=== 2. 清理临时文件 ===")
        ssh.exec_command("rm -rf /tmp/flink-*")
        for slave in ['slave1', 'slave2']:
            ssh.exec_command(f"ssh {slave} 'rm -rf /tmp/flink-*'")
        
        # 启动集群
        print("\n=== 3. 启动Flink集群 ===")
        stdin, stdout, stderr = ssh.exec_command("cd /opt/module/flink && bin/start-cluster.sh")
        output = stdout.read().decode().strip()
        print(output)
        
        time.sleep(8)
        
        # 检查进程
        print("\n=== 4. 检查Flink进程 ===")
        stdin, stdout, stderr = ssh.exec_command("jps")
        print("master节点:")
        print(stdout.read().decode().strip())
        
        for slave in ['slave1', 'slave2']:
            stdin, stdout, stderr = ssh.exec_command(f"ssh {slave} 'jps | grep TaskManager'")
            result = stdout.read().decode().strip()
            print(f"{slave}节点: {result}")
        
        # 检查端口
        print("\n=== 5. 检查端口 ===")
        stdin, stdout, stderr = ssh.exec_command("netstat -tlnp | grep 8082")
        print(stdout.read().decode().strip() or "8082未监听")
        
        # 提交作业
        print("\n=== 6. 提交Flink作业 ===")
        flink_cmd = (
            "cd /opt/module/flink && "
            "bin/flink run -d "
            "-Drest.port=8082 "
            "-c org.example.tasks.FlinkRulInference "
            "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
        )
        stdin, stdout, stderr = ssh.exec_command(flink_cmd)
        result = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        
        print("输出:")
        print(result[-800:] if len(result) > 800 else result)
        if error:
            print("\n错误:", error[-400:] if len(error) > 400 else error)
        
        time.sleep(5)
        
        # 检查作业
        print("\n=== 7. 检查作业状态 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cd /opt/module/flink && bin/flink list -Drest.port=8082 2>&1"
        )
        print(stdout.read().decode().strip())
        
        ssh.close()
        print("\n=== 完成 ===")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    restart_flink()
