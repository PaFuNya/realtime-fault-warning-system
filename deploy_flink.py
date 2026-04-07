#!/usr/bin/env python3
import paramiko
import sys
import os
from scp import SCPClient

def deploy_flink():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        # 上传JAR包
        print("=== 1. 上传Flink JAR包 ===")
        local_jar = "d:\\Desktop\\Match\\MATCH\\OverMatch\\target\\realtime-data-process-1.0-SNAPSHOT.jar"
        remote_jar = "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
        
        with SCPClient(ssh.get_transport()) as scp:
            print(f"上传 {local_jar} 到 {remote_jar}...")
            scp.put(local_jar, remote_jar)
        print("上传完成")
        
        # 检查Flink是否运行
        print("\n=== 2. 检查Flink集群状态 ===")
        stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'Standalone|TaskManager'")
        flink_result = stdout.read().decode().strip()
        if not flink_result:
            print("Flink未运行，启动集群...")
            stdin, stdout, stderr = ssh.exec_command("cd /opt/module/flink && bin/start-cluster.sh")
            import time
            time.sleep(5)
        else:
            print(f"Flink已在运行: {flink_result}")
        
        # 提交Flink作业
        print("\n=== 3. 提交Flink作业 ===")
        flink_cmd = (
            "cd /opt/module/flink && "
            "bin/flink run -d "  # -d表示后台运行
            "-c org.example.tasks.FlinkRulInference "
            f"{remote_jar}"
        )
        print(f"执行: {flink_cmd}")
        stdin, stdout, stderr = ssh.exec_command(flink_cmd)
        result = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        
        if error:
            print(f"错误: {error}")
        if result:
            print(f"结果: {result}")
        
        # 等待作业启动
        import time
        time.sleep(3)
        
        # 检查作业状态
        print("\n=== 4. 检查作业状态 ===")
        stdin, stdout, stderr = ssh.exec_command("cd /opt/module/flink && bin/flink list")
        job_status = stdout.read().decode().strip()
        print(job_status)
        
        # 检查数据生成状态
        print("\n=== 5. 检查数据生成状态 ===")
        stdin, stdout, stderr = ssh.exec_command("ps aux | grep generate | grep -v grep | wc -l")
        gen_count = stdout.read().decode().strip()
        print(f"数据生成脚本数量: {gen_count}")
        
        # 检查高频日志
        print("\n=== 6. 高频数据日志最新5行 ===")
        stdin, stdout, stderr = ssh.exec_command("tail -5 /data_log/highfreq_sensor_generated.log 2>/dev/null")
        print(stdout.read().decode().strip())
        
        ssh.close()
        print("\n=== 部署完成 ===")
        print("\n等待3分钟后，高频窗口将触发，方差和峭度将被计算")
        print("可以查看Flink日志确认: tail -f /opt/module/flink/log/flink-*-taskexecutor-*.out")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    deploy_flink()
