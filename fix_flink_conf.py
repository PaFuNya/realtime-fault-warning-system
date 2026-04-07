#!/usr/bin/env python3
import paramiko
import sys
import time

def fix_flink_conf():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!\n")
        
        flink_conf = "/opt/module/flink/conf/flink-conf.yaml"
        
        # 停止占用8081的进程（如果是Spark的话）
        print("=== 1. 检查占用8081的进程 ===")
        stdin, stdout, stderr = ssh.exec_command("netstat -tlnp | grep 8081")
        result = stdout.read().decode().strip()
        print(result)
        
        if "java" in result:
            # 获取PID
            pid = result.split()[-1].split('/')[0]
            print(f"进程PID: {pid}")
            
        # 正确修改Flink配置 - 使用rest.bind-port
        print("\n=== 2. 修改Flink配置 ===")
        
        # 先备份
        stdin, stdout, stderr = ssh.exec_command(f"cp {flink_conf} {flink_conf}.backup")
        
        # 删除旧的rest.port配置（如果有）
        stdin, stdout, stderr = ssh.exec_command(f"sed -i '/^rest.port:/d' {flink_conf}")
        
        # 添加rest.bind-port配置（范围格式）
        stdin, stdout, stderr = ssh.exec_command(
            f"echo 'rest.bind-port: 8082-8090' >> {flink_conf}"
        )
        
        # 验证修改
        stdin, stdout, stderr = ssh.exec_command(f"grep -E 'rest' {flink_conf}")
        print("REST配置:")
        print(stdout.read().decode().strip())
        
        # 停止所有Flink进程
        print("\n=== 3. 停止所有Flink进程 ===")
        ssh.exec_command("pkill -9 -f 'flink|Standalone|TaskManager' 2>/dev/null")
        time.sleep(3)
        
        # 检查是否还有Flink进程
        stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'Standalone|TaskManager'")
        print("剩余Flink进程:", stdout.read().decode().strip() or "无")
        
        # 清理临时文件
        print("\n=== 4. 清理临时文件 ===")
        ssh.exec_command("rm -rf /tmp/flink-*")
        
        # 启动Flink
        print("\n=== 5. 启动Flink集群 ===")
        stdin, stdout, stderr = ssh.exec_command("cd /opt/module/flink && bin/start-cluster.sh")
        output = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print("输出:", output)
        if error:
            print("错误:", error)
        
        time.sleep(5)
        
        # 检查进程
        print("\n=== 6. 检查Flink进程 ===")
        stdin, stdout, stderr = ssh.exec_command("jps")
        print(stdout.read().decode().strip())
        
        # 检查端口
        print("\n=== 7. 检查端口 ===")
        stdin, stdout, stderr = ssh.exec_command("netstat -tlnp | grep -E '808[12]'")
        print(stdout.read().decode().strip() or "无端口监听")
        
        # 检查JobManager日志
        print("\n=== 8. 检查JobManager日志 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cat /opt/module/flink/log/flink-*-standalonesession-*.log 2>/dev/null | tail -20"
        )
        log = stdout.read().decode().strip()
        print(log[-500:] if len(log) > 500 else log)
        
        ssh.close()
        print("\n=== 完成 ===")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    fix_flink_conf()
