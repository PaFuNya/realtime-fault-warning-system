#!/usr/bin/env python3
import paramiko
import sys
import time

def start_flink_job():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        # 检查所有Java进程
        print("=== 1. 检查所有Java进程 ===")
        stdin, stdout, stderr = ssh.exec_command("jps -l")
        print(stdout.read().decode().strip())
        
        # 手动启动JobManager
        print("\n=== 2. 启动JobManager ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cd /opt/module/flink && nohup bin/jobmanager.sh start-foreground > /tmp/flink-jobmanager.log 2>&1 &"
        )
        time.sleep(5)
        
        # 检查JobManager
        print("\n=== 3. 检查JobManager ===")
        stdin, stdout, stderr = ssh.exec_command("jps | grep Standalone")
        result = stdout.read().decode().strip()
        print(result if result else "JobManager未启动")
        
        if not result:
            print("查看日志...")
            stdin, stdout, stderr = ssh.exec_command("cat /tmp/flink-jobmanager.log 2>/dev/null | tail -30")
            print(stdout.read().decode().strip())
        
        # 启动TaskManager
        print("\n=== 4. 启动TaskManager ===")
        stdin, stdout, stderr = ssh.exec_command("cd /opt/module/flink && bin/taskmanager.sh start")
        time.sleep(3)
        
        # 检查TaskManager
        print("\n=== 5. 检查TaskManager ===")
        stdin, stdout, stderr = ssh.exec_command("jps | grep TaskManager")
        result = stdout.read().decode().strip()
        print(result if result else "TaskManager未启动")
        
        # 检查端口
        print("\n=== 6. 检查端口 ===")
        stdin, stdout, stderr = ssh.exec_command("netstat -tlnp | grep -E '8081|8082'")
        print(stdout.read().decode().strip() or "无端口监听")
        
        # 尝试提交作业
        print("\n=== 7. 提交Flink作业 ===")
        # 设置环境变量指向正确的REST端口
        flink_cmd = (
            "export FLINK_REST_PORT=8082; "
            "cd /opt/module/flink && "
            "bin/flink run -d "
            "-c org.example.tasks.FlinkRulInference "
            "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
        )
        stdin, stdout, stderr = ssh.exec_command(flink_cmd)
        result = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        
        print("输出:", result[-800:] if len(result) > 800 else result)
        if error:
            print("错误:", error[-500:] if len(error) > 500 else error)
        
        time.sleep(5)
        
        # 检查作业
        print("\n=== 8. 检查作业状态 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cd /opt/module/flink && bin/flink list 2>&1 | tail -20"
        )
        print(stdout.read().decode().strip())
        
        # 检查TaskManager日志
        print("\n=== 9. 检查TaskManager日志 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cat /opt/module/flink/log/flink-*-taskexecutor-*.out 2>/dev/null | tail -30"
        )
        log_content = stdout.read().decode().strip()
        if log_content:
            print(log_content)
        else:
            print("暂无日志输出")
        
        ssh.close()
        print("\n=== 完成 ===")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    start_flink_job()
