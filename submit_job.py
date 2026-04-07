#!/usr/bin/env python3
import paramiko
import sys
import time

def submit_job():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        # 检查Flink是否运行
        print("=== 1. 检查Flink集群 ===")
        stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'Standalone|TaskManager'")
        result = stdout.read().decode().strip()
        print(result if result else "Flink未运行")
        
        if not result:
            print("Flink未运行，退出")
            return
        
        # 检查数据生成脚本
        print("\n=== 2. 检查数据生成脚本 ===")
        stdin, stdout, stderr = ssh.exec_command("ps aux | grep generate | grep -v grep")
        print(stdout.read().decode().strip() or "无数据生成脚本")
        
        # 提交Flink作业
        print("\n=== 3. 提交Flink作业 ===")
        # 使用-P参数指定REST端口
        flink_cmd = (
            "cd /opt/module/flink && "
            "bin/flink run -d -p 8082 "
            "-c org.example.tasks.FlinkRulInference "
            "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
        )
        stdin, stdout, stderr = ssh.exec_command(flink_cmd)
        result = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        
        print("输出:")
        print(result[-1000:] if len(result) > 1000 else result)
        if error:
            print("\n错误:")
            print(error[-500:] if len(error) > 500 else error)
        
        time.sleep(5)
        
        # 检查作业状态
        print("\n=== 4. 检查作业状态 ===")
        stdin, stdout, stderr = ssh.exec_command("cd /opt/module/flink && bin/flink list -p 8082")
        job_status = stdout.read().decode().strip()
        print(job_status)
        
        # 如果作业提交成功，等待并检查日志
        if "Flink-FeatureEngine" in job_status or "RUNNING" in job_status:
            print("\n=== 5. 等待高频窗口触发（约3分钟） ===")
            print("高频数据正在生成，等待窗口聚合...")
            
            # 等待窗口触发
            for i in range(6):  # 等待约3分钟
                time.sleep(30)
                print(f"等待中... { (i+1)*30 }秒")
                
                # 检查TaskManager日志
                stdin, stdout, stderr = ssh.exec_command(
                    "cat /opt/module/flink/log/flink-*-taskexecutor-*.out 2>/dev/null | grep -E 'var_temp|kurtosis|高频窗口' | tail -5"
                )
                logs = stdout.read().decode().strip()
                if logs:
                    print("\n最新日志:")
                    print(logs)
        else:
            print("\n作业可能未成功提交，检查日志...")
            stdin, stdout, stderr = ssh.exec_command(
                "cat /opt/module/flink/log/flink-*-standalonesession-*.log 2>/dev/null | tail -30"
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
    submit_job()
