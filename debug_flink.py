#!/usr/bin/env python3
import paramiko
import sys

def debug_flink():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        # 检查高频数据是否发送到Kafka
        print("=== 1. 检查高频数据是否发送到Kafka ===")
        stdin, stdout, stderr = ssh.exec_command(
            "kafka-console-consumer.sh --bootstrap-server master:9092 --topic highfreq_sensor --from-beginning --max-messages 5 --timeout-ms 5000 2>/dev/null"
        )
        kafka_data = stdout.read().decode().strip()
        if kafka_data:
            print("Kafka highfreq_sensor topic 数据:")
            for line in kafka_data.split('\n')[:3]:
                print(f"  {line[:150]}...")
        else:
            print("暂无数据或无法读取")
        
        # 检查Flink日志中是否有高频原始数据
        print("\n=== 2. 检查Flink是否消费到高频数据 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cat /opt/module/flink/log/flink-*-taskexecutor-*.out 2>/dev/null | grep '高频原始数据' | head -5"
        )
        raw_logs = stdout.read().decode().strip()
        if raw_logs:
            print("Flink消费到的高频原始数据:")
            print(raw_logs)
        else:
            print("未找到'高频原始数据'日志")
        
        # 检查高频数据解析
        print("\n=== 3. 检查高频数据解析 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cat /opt/module/flink/log/flink-*-taskexecutor-*.out 2>/dev/null | grep '高频解析成功' | head -5"
        )
        parse_logs = stdout.read().decode().strip()
        if parse_logs:
            print("解析成功日志:")
            print(parse_logs)
        else:
            print("未找到'高频解析成功'日志")
        
        # 检查高频数据流入
        print("\n=== 4. 检查高频数据流入 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cat /opt/module/flink/log/flink-*-taskexecutor-*.out 2>/dev/null | grep '高频数据' | head -5"
        )
        data_logs = stdout.read().decode().strip()
        if data_logs:
            print("数据流入日志:")
            print(data_logs)
        else:
            print("未找到'高频数据'日志")
        
        # 检查窗口聚合日志
        print("\n=== 5. 检查窗口聚合日志（完整） ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cat /opt/module/flink/log/flink-*-taskexecutor-*.out 2>/dev/null | grep '窗口聚合完成'"
        )
        window_logs = stdout.read().decode().strip()
        if window_logs:
            print("窗口聚合日志:")
            print(window_logs)
        else:
            print("未找到窗口聚合日志")
        
        # 检查生成的日志文件中的温度变化
        print("\n=== 6. 检查生成的日志文件中的温度变化 ===")
        stdin, stdout, stderr = ssh.exec_command(
            'cat /data_log/highfreq_sensor_generated.log | head -20 | grep -o \'temperature\":[0-9.]*\' | sort -u | wc -l'
        )
        unique_temps = stdout.read().decode().strip()
        print(f"前20条数据中有 {unique_temps} 个不同的温度值")
        
        stdin, stdout, stderr = ssh.exec_command(
            'head -5 /data_log/highfreq_sensor_generated.log | grep -o \'temperature\":[0-9.]*\''
        )
        temps = stdout.read().decode().strip()
        print(f"前5条数据的温度值: {temps}")
        
        ssh.close()
        print("\n=== 调试完成 ===")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    debug_flink()
