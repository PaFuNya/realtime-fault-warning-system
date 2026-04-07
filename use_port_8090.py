#!/usr/bin/env python3
import paramiko
import sys
import time

def use_port_8090():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        flink_conf = "/opt/module/flink/conf/flink-conf.yaml"
        
        # 检查哪些端口被占用
        print("=== 1. 检查端口占用情况 ===")
        for port in [8081, 8082, 8083, 8090, 8091]:
            stdin, stdout, stderr = ssh.exec_command(f"netstat -tlnp 2>/dev/null | grep ':{port} '")
            result = stdout.read().decode().strip()
            if result:
                print(f"端口 {port}: 被占用")
            else:
                print(f"端口 {port}: 可用")
        
        # 修改配置使用8090
        print("\n=== 2. 修改Flink配置使用8090端口 ===")
        
        # 删除旧的rest配置
        ssh.exec_command(f"sed -i '/^rest/d' {flink_conf}")
        ssh.exec_command(f"sed -i '/^#rest/d' {flink_conf}")
        
        # 添加新配置
        ssh.exec_command(f"echo '' >> {flink_conf}")
        ssh.exec_command(f"echo '# REST API配置' >> {flink_conf}")
        ssh.exec_command(f"echo 'rest.bind-port: 8090' >> {flink_conf}")
        ssh.exec_command(f"echo 'rest.address: 0.0.0.0' >> {flink_conf}")
        
        # 验证
        stdin, stdout, stderr = ssh.exec_command(f"grep -E '^rest' {flink_conf}")
        print("REST配置:")
        print(stdout.read().decode().strip())
        
        # 停止所有Flink
        print("\n=== 3. 停止Flink ===")
        ssh.exec_command("cd /opt/module/flink && bin/stop-cluster.sh 2>/dev/null")
        ssh.exec_command("pkill -9 -f flink 2>/dev/null")
        time.sleep(3)
        
        # 清理
        print("=== 4. 清理临时文件 ===")
        ssh.exec_command("rm -rf /tmp/flink-*")
        
        # 启动
        print("\n=== 5. 启动Flink集群 ===")
        stdin, stdout, stderr = ssh.exec_command("cd /opt/module/flink && bin/start-cluster.sh")
        print(stdout.read().decode().strip())
        
        time.sleep(8)
        
        # 检查
        print("\n=== 6. 检查Flink进程 ===")
        stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'Standalone|TaskManager'")
        print(stdout.read().decode().strip() or "无进程")
        
        # 检查8090端口
        print("\n=== 7. 检查8090端口 ===")
        stdin, stdout, stderr = ssh.exec_command("netstat -tlnp | grep 8090")
        print(stdout.read().decode().strip() or "8090未监听")
        
        # 提交作业
        print("\n=== 8. 提交Flink作业 ===")
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
        
        # 检查作业
        print("\n=== 9. 检查作业状态 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cd /opt/module/flink && bin/flink list -Drest.port=8090 2>&1 | head -20"
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
    use_port_8090()
