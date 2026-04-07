#!/usr/bin/env python3
import paramiko
import sys
import time
from scp import SCPClient

def restart_job():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        # 停止当前Flink作业
        print("=== 1. 停止当前Flink作业 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cd /opt/module/flink && bin/flink list -Drest.port=8090 2>&1 | grep RUNNING"
        )
        job_line = stdout.read().decode().strip()
        if job_line:
            # 提取JobID
            parts = job_line.split()
            if len(parts) >= 4:
                job_id = parts[2]
                print(f"停止作业 {job_id}...")
                ssh.exec_command(f"cd /opt/module/flink && bin/flink cancel -Drest.port=8090 {job_id}")
                time.sleep(3)
        else:
            print("没有运行中的作业")
        
        # 上传新的JAR包
        print("\n=== 2. 上传新的JAR包 ===")
        local_jar = "d:\\Desktop\\Match\\MATCH\\OverMatch\\target\\realtime-data-process-1.0-SNAPSHOT.jar"
        remote_jar = "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
        
        with SCPClient(ssh.get_transport()) as scp:
            scp.put(local_jar, remote_jar)
        print("上传完成")
        
        # 清空Kafka旧数据（可选）
        print("\n=== 3. 清空高频topic旧数据 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "kafka-topics.sh --bootstrap-server master:9092 --delete --topic highfreq_sensor 2>&1"
        )
        time.sleep(2)
        stdin, stdout, stderr = ssh.exec_command(
            "kafka-topics.sh --bootstrap-server master:9092 --create --topic highfreq_sensor --partitions 1 --replication-factor 1 2>&1"
        )
        print("已重置highfreq_sensor topic")
        
        # 重启数据生成脚本
        print("\n=== 4. 重启数据生成脚本 ===")
        ssh.exec_command("pkill -f generate_highfreq_sensor")
        ssh.exec_command("pkill -f generate_change_data")
        time.sleep(2)
        ssh.exec_command("cd /data_log && nohup bash generate_change_data.sh > /dev/null 2>&1 &")
        ssh.exec_command("cd /data_log && nohup bash generate_highfreq_sensor.sh 1 > /dev/null 2>&1 &")
        print("数据生成脚本已重启")
        
        time.sleep(3)
        
        # 提交新的Flink作业
        print("\n=== 5. 提交新的Flink作业 ===")
        flink_cmd = (
            "cd /opt/module/flink && "
            "bin/flink run -d "
            "-Drest.port=8090 "
            "-c org.example.tasks.FlinkRulInference "
            "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
        )
        stdin, stdout, stderr = ssh.exec_command(flink_cmd)
        result = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        
        print("输出:", result[-600:] if len(result) > 600 else result)
        if error:
            print("错误:", error[-300:] if len(error) > 300 else error)
        
        time.sleep(5)
        
        # 检查作业状态
        print("\n=== 6. 检查作业状态 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cd /opt/module/flink && bin/flink list -Drest.port=8090 2>&1 | head -15"
        )
        print(stdout.read().decode().strip())
        
        # 清空TaskManager日志
        print("\n=== 7. 清空TaskManager日志 ===")
        ssh.exec_command("> /opt/module/flink/log/flink-*-taskexecutor-*.out 2>/dev/null")
        
        ssh.close()
        print("\n=== 完成 ===")
        print("\n请等待3分钟后，高频窗口将触发，var_temp 和 kurtosis_temp 应该能正常计算")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    restart_job()
