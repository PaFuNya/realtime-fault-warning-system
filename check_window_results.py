#!/usr/bin/env python3
import paramiko
import sys
import time

def check_window_results():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        print("=== Flink作业已运行，等待高频窗口触发 ===")
        print("窗口大小: 3分钟")
        print("预计首次窗口触发时间: 3分钟后\n")
        
        # 等待并监控
        for i in range(12):  # 监控6分钟
            elapsed = (i + 1) * 30
            print(f"\n--- 已等待 {elapsed} 秒 ---")
            
            # 检查TaskManager日志
            stdin, stdout, stderr = ssh.exec_command(
                "cat /opt/module/flink/log/flink-*-taskexecutor-*.out 2>/dev/null"
            )
            logs = stdout.read().decode().strip()
            
            # 查找关键日志
            if logs:
                # 查找高频窗口相关日志
                import re
                highfreq_logs = [line for line in logs.split('\n') if '高频窗口' in line or 'var_temp' in line or 'kurtosis' in line]
                if highfreq_logs:
                    print("高频窗口日志:")
                    for log in highfreq_logs[-5:]:  # 显示最后5条
                        print(f"  {log}")
                
                # 查找合并日志
                merge_logs = [line for line in logs.split('\n') if '合并' in line]
                if merge_logs:
                    print("合并日志:")
                    for log in merge_logs[-3:]:
                        print(f"  {log}")
            
            # 检查数据生成状态
            stdin, stdout, stderr = ssh.exec_command("tail -3 /data_log/highfreq_sensor_generated.log 2>/dev/null")
            latest_data = stdout.read().decode().strip()
            if latest_data:
                print(f"最新高频数据: {latest_data.split(chr(10))[-1][:100]}...")
            
            # 检查作业状态
            stdin, stdout, stderr = ssh.exec_command(
                "cd /opt/module/flink && bin/flink list -Drest.port=8090 2>&1 | grep -E 'RUNNING|JobID'"
            )
            status = stdout.read().decode().strip()
            if status:
                print(f"作业状态: {status}")
            
            if i < 11:  # 最后一次不等待
                time.sleep(30)
        
        # 最终检查
        print("\n\n=== 最终结果检查 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cat /opt/module/flink/log/flink-*-taskexecutor-*.out 2>/dev/null | grep -E 'var_temp|kurtosis_temp' | tail -10"
        )
        final_results = stdout.read().decode().strip()
        if final_results:
            print("var_temp 和 kurtosis_temp 计算结果:")
            print(final_results)
        else:
            print("尚未看到var_temp/kurtosis_temp计算结果，可能需要更长时间")
        
        # 检查warning_log topic是否有数据
        print("\n=== 检查Kafka输出 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "kafka-console-consumer.sh --bootstrap-server master:9092 --topic warning_log --from-beginning --max-messages 3 --timeout-ms 5000 2>/dev/null || echo '无法读取或暂无数据'"
        )
        kafka_output = stdout.read().decode().strip()
        if kafka_output and '无法读取' not in kafka_output:
            print("warning_log topic 数据:")
            print(kafka_output[:500])
        
        ssh.close()
        print("\n=== 监控完成 ===")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    check_window_results()
