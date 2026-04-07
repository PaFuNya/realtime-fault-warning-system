#!/usr/bin/env python3
import paramiko
import sys
import time

def start_services():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        # 查找Kafka安装目录
        print("=== 1. 查找Kafka安装目录 ===")
        stdin, stdout, stderr = ssh.exec_command("find / -name 'kafka-server-start.sh' 2>/dev/null | head -1")
        kafka_start = stdout.read().decode().strip()
        if kafka_start:
            kafka_home = kafka_start.replace('/bin/kafka-server-start.sh', '').replace('/kafka-server-start.sh', '')
            print(f"找到Kafka: {kafka_home}")
        else:
            print("未找到Kafka，尝试常用路径")
            kafka_home = "/opt/kafka"
        
        # 启动Zookeeper
        print("\n=== 2. 启动Zookeeper ===")
        stdin, stdout, stderr = ssh.exec_command(f"jps | grep Quorum")
        if not stdout.read().decode().strip():
            stdin, stdout, stderr = ssh.exec_command(f"cd {kafka_home} && nohup bin/zookeeper-server-start.sh config/zookeeper.properties > /dev/null 2>&1 &")
            time.sleep(3)
            print("Zookeeper已启动")
        else:
            print("Zookeeper已在运行")
        
        # 启动Kafka
        print("\n=== 3. 启动Kafka ===")
        stdin, stdout, stderr = ssh.exec_command("jps | grep Kafka")
        if not stdout.read().decode().strip():
            stdin, stdout, stderr = ssh.exec_command(f"cd {kafka_home} && nohup bin/kafka-server-start.sh config/server.properties > /dev/null 2>&1 &")
            time.sleep(5)
            print("Kafka已启动")
        else:
            print("Kafka已在运行")
        
        # 创建必要的topics
        print("\n=== 4. 创建Kafka Topics ===")
        topics = ["device_state", "sensor_metrics", "highfreq_sensor", "warning_log"]
        for topic in topics:
            stdin, stdout, stderr = ssh.exec_command(
                f"cd {kafka_home} && bin/kafka-topics.sh --create --if-not-exists --topic {topic} --bootstrap-server master:9092 --partitions 1 --replication-factor 1 2>&1"
            )
            result = stdout.read().decode().strip()
            print(f"  Topic '{topic}': {result if result else '已存在或创建成功'}")
        
        # 查找Flink安装目录
        print("\n=== 5. 查找Flink安装目录 ===")
        stdin, stdout, stderr = ssh.exec_command("find / -name 'start-cluster.sh' 2>/dev/null | grep flink | head -1")
        flink_start = stdout.read().decode().strip()
        if flink_start:
            flink_home = flink_start.replace('/bin/start-cluster.sh', '').replace('/start-cluster.sh', '')
            print(f"找到Flink: {flink_home}")
        else:
            print("未找到Flink，尝试常用路径")
            flink_home = "/opt/flink"
        
        # 启动Flink集群
        print("\n=== 6. 启动Flink集群 ===")
        stdin, stdout, stderr = ssh.exec_command("jps | grep Standalone")
        if not stdout.read().decode().strip():
            stdin, stdout, stderr = ssh.exec_command(f"cd {flink_home} && bin/start-cluster.sh")
            time.sleep(5)
            print("Flink集群已启动")
        else:
            print("Flink已在运行")
        
        # 检查Flink Web UI
        print("\n=== 7. 验证服务状态 ===")
        stdin, stdout, stderr = ssh.exec_command("jps")
        print("Java进程:")
        print(stdout.read().decode().strip())
        
        # 列出topics
        print("\n=== 8. 列出所有Kafka Topics ===")
        stdin, stdout, stderr = ssh.exec_command(f"cd {kafka_home} && bin/kafka-topics.sh --list --bootstrap-server master:9092")
        print(stdout.read().decode().strip())
        
        ssh.close()
        print("\n=== 服务启动完成 ===")
        print("\n下一步:")
        print("1. 编译并上传Flink作业JAR包")
        print("2. 运行: flink run -c org.example.tasks.FlinkRulInference your-jar.jar")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    start_services()
