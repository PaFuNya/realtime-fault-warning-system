#!/usr/bin/env python3
import paramiko
import sys
import time

def fix_port_conflict():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        # 检查8081端口被谁占用
        print("=== 1. 检查8081端口占用 ===")
        stdin, stdout, stderr = ssh.exec_command("netstat -tlnp | grep 8081")
        result = stdout.read().decode().strip()
        print(result if result else "端口8081未被占用")
        
        # 修改Flink配置使用8082端口
        print("\n=== 2. 修改Flink REST端口为8082 ===")
        flink_conf = "/opt/module/flink/conf/flink-conf.yaml"
        
        # 检查配置文件中是否有rest.port
        stdin, stdout, stderr = ssh.exec_command(f"grep 'rest.port' {flink_conf}")
        rest_port_line = stdout.read().decode().strip()
        
        if rest_port_line:
            # 替换现有配置
            stdin, stdout, stderr = ssh.exec_command(
                f"sed -i 's/rest.port:.*/rest.port: 8082/' {flink_conf}"
            )
        else:
            # 添加新配置
            stdin, stdout, stderr = ssh.exec_command(
                f"echo 'rest.port: 8082' >> {fllink_conf}"
            )
        
        # 验证修改
        stdin, stdout, stderr = ssh.exec_command(f"grep 'rest.port' {flink_conf}")
        print(f"REST端口配置: {stdout.read().decode().strip()}")
        
        # 停止所有进程
        print("\n=== 3. 停止所有Flink进程 ===")
        ssh.exec_command("pkill -f flink; pkill -f TaskManager; pkill -f Standalone")
        time.sleep(3)
        
        # 清理临时文件
        print("=== 4. 清理临时文件 ===")
        ssh.exec_command("rm -rf /tmp/flink-*")
        
        # 启动Flink
        print("\n=== 5. 启动Flink集群 ===")
        stdin, stdout, stderr = ssh.exec_command("cd /opt/module/flink && bin/start-cluster.sh")
        time.sleep(5)
        
        # 检查进程
        print("\n=== 6. 检查Flink进程 ===")
        stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'Standalone|TaskManager'")
        result = stdout.read().decode().strip()
        print(result if result else "Flink未启动")
        
        # 检查8082端口
        print("\n=== 7. 检查8082端口 ===")
        stdin, stdout, stderr = ssh.exec_command("netstat -tlnp | grep 8082")
        print(stdout.read().decode().strip() or "8082端口未监听")
        
        # 提交作业
        if result:
            print("\n=== 8. 提交Flink作业 ===")
            flink_cmd = (
                "cd /opt/module/flink && "
                "bin/flink run -d "
                "-c org.example.tasks.FlinkRulInference "
                "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
            )
            stdin, stdout, stderr = ssh.exec_command(flink_cmd)
            result = stdout.read().decode().strip()
            error = stderr.read().decode().strip()
            
            print("输出:", result[-500:] if len(result) > 500 else result)
            if error:
                print("错误:", error[-500:] if len(error) > 500 else error)
            
            time.sleep(3)
            
            # 检查作业
            print("\n=== 9. 检查作业状态 ===")
            stdin, stdout, stderr = ssh.exec_command("cd /opt/module/flink && bin/flink list")
            print(stdout.read().decode().strip())
        
        ssh.close()
        print("\n=== 完成 ===")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    fix_port_conflict()
