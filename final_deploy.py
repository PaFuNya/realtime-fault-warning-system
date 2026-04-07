#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
上传JAR、修复Flink端口并提交作业
"""

import paramiko
import time

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 最终部署 ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 上传JAR文件
    print("\n=== 1. 上传JAR文件 ===")
    jar_local = "d:/Desktop/Match/MATCH/OverMatch/target/realtime-data-process-1.0-SNAPSHOT.jar"
    jar_remote = "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
    
    transport = ssh.get_transport()
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        sftp.put(jar_local, jar_remote)
        print(f"[OK] JAR已上传")
    except Exception as e:
        print(f"[FAIL] 上传失败: {e}")
        sftp.close()
        ssh.close()
        return
    sftp.close()
    
    # 2. 修复Flink端口配置（使用8084避免冲突）
    print("\n=== 2. 修复Flink端口配置 ===")
    ssh.exec_command("pkill -f StandaloneSession")
    ssh.exec_command("pkill -f TaskManager")
    time.sleep(3)
    
    # 修改配置
    ssh.exec_command("sed -i 's/^rest.port:.*/rest.port: 8084/' /opt/module/flink/conf/flink-conf.yaml")
    ssh.exec_command("sed -i '/^rest.port:/!{ /^#rest.port:/!a rest.port: 8084 }' /opt/module/flink/conf/flink-conf.yaml")
    
    # 验证配置
    stdin, stdout, stderr = ssh.exec_command("grep 'rest.port' /opt/module/flink/conf/flink-conf.yaml")
    print(f"端口配置: {stdout.read().decode().strip()}")
    
    # 3. 启动Flink集群
    print("\n=== 3. 启动Flink集群 ===")
    stdin, stdout, stderr = ssh.exec_command("/opt/module/flink/bin/start-cluster.sh 2>&1")
    output = stdout.read().decode()
    print(output)
    time.sleep(5)
    
    # 检查进程
    stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'StandaloneSession|TaskManager'")
    processes = stdout.read().decode().strip()
    if processes:
        print(f"[OK] Flink进程:\n{processes}")
    else:
        print("[FAIL] Flink未启动")
        ssh.close()
        return
    
    # 4. 等待REST服务就绪
    print("\n=== 4. 等待REST服务就绪 ===")
    for i in range(10):
        stdin, stdout, stderr = ssh.exec_command("curl -s http://localhost:8084 2>/dev/null | head -1")
        response = stdout.read().decode()
        if response:
            print(f"[OK] REST服务就绪: {response[:50]}")
            break
        time.sleep(2)
    else:
        print("[WARN] REST服务可能未就绪，继续尝试提交作业...")
    
    # 5. 提交Flink作业
    print("\n=== 5. 提交Flink作业 ===")
    # 设置环境变量使用正确的端口
    cmd = f"export FLINK_REST_PORT=8084 && /opt/module/flink/bin/flink run -d {jar_remote} 2>&1"
    print(f"执行: {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    result = stdout.read().decode()
    print(result)
    
    time.sleep(5)
    
    # 6. 检查作业状态
    print("\n=== 6. 检查作业状态 ===")
    stdin, stdout, stderr = ssh.exec_command("export FLINK_REST_PORT=8084 && /opt/module/flink/bin/flink list 2>&1")
    job_status = stdout.read().decode()
    print(job_status)
    
    if "RUNNING" in job_status:
        print("\n[OK] Flink作业已成功启动并运行！")
        print("\n等待3分钟让高频窗口触发...")
        for i in range(3):
            time.sleep(60)
            print(f"  已等待 {i+1} 分钟...")
        
        # 检查窗口计算结果
        print("\n=== 7. 检查窗口计算结果 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "find /opt/module/flink/log -name '*.out' 2>/dev/null | head -1 | xargs grep -E 'var_temp|kurtosis_temp' 2>/dev/null | tail -5"
        )
        results = stdout.read().decode().strip()
        if results:
            print("找到计算结果:")
            for line in results.split('\n'):
                print(f"  {line}")
        else:
            print("未找到var_temp/kurtosis_temp，检查Flink输出...")
            stdin, stdout, stderr = ssh.exec_command(
                "find /opt/module/flink/log -name '*.out' 2>/dev/null | head -1 | xargs tail -50 2>/dev/null"
            )
            output = stdout.read().decode()
            if output:
                lines = [l for l in output.split('\n') if '高频' in l or '窗口' in l or '特征' in l]
                for line in lines[-10:]:
                    print(f"  {line}")
    else:
        print("\n[WARN] 作业未正常运行")
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
