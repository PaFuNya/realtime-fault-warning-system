#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
上传JAR并部署Flink作业
"""

import paramiko
import time

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 上传JAR并部署Flink作业 ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 上传JAR文件
    print("\n=== 1. 上传JAR文件 ===")
    jar_local = "d:/Desktop/Match/MATCH/OverMatch/target/realtime-data-process-1.0-SNAPSHOT.jar"
    jar_remote = "/opt/module/flink/realtime-data-process-1.0-SNAPSHOT.jar"
    
    transport = ssh.get_transport()
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        sftp.put(jar_local, jar_remote)
        print(f"[OK] JAR已上传到 {jar_remote}")
    except Exception as e:
        print(f"[FAIL] 上传失败: {e}")
        sftp.close()
        ssh.close()
        return
    sftp.close()
    
    # 2. 检查Flink并启动
    print("\n=== 2. 启动Flink集群 ===")
    stdin, stdout, stderr = ssh.exec_command("jps | grep StandaloneSession")
    if not stdout.read().decode().strip():
        print("Flink未运行，正在启动...")
        # 确保端口配置正确
        ssh.exec_command("grep 'rest.port' /opt/module/flink/conf/flink-conf.yaml")
        # 启动集群
        stdin, stdout, stderr = ssh.exec_command("/opt/module/flink/bin/start-cluster.sh 2>&1")
        output = stdout.read().decode()
        if output:
            print(f"启动输出: {output}")
        time.sleep(5)
        
        # 检查进程
        stdin, stdout, stderr = ssh.exec_command("jps | grep -E 'StandaloneSession|TaskManager'")
        processes = stdout.read().decode().strip()
        if processes:
            print(f"[OK] Flink进程:\n{processes}")
        else:
            print("[FAIL] Flink启动失败，检查日志...")
            stdin, stdout, stderr = ssh.exec_command("tail -30 /opt/module/flink/log/flink-*.log")
            print(stdout.read().decode())
            ssh.close()
            return
    else:
        print("Flink已在运行")
    
    # 3. 取消现有作业
    print("\n=== 3. 取消现有作业 ===")
    stdin, stdout, stderr = ssh.exec_command("/opt/module/flink/bin/flink list 2>&1")
    jobs = stdout.read().decode()
    print(f"当前作业:\n{jobs}")
    
    if "RUNNING" in jobs or "CREATED" in jobs or "RESTARTING" in jobs:
        import re
        job_ids = re.findall(r'([a-f0-9]{32})', jobs)
        for job_id in job_ids:
            print(f"取消作业 {job_id}...")
            ssh.exec_command(f"/opt/module/flink/bin/flink cancel {job_id}")
            time.sleep(1)
    
    # 4. 提交新作业
    print("\n=== 4. 提交Flink作业 ===")
    cmd = f"/opt/module/flink/bin/flink run -d {jar_remote} 2>&1"
    print(f"执行: {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    result = stdout.read().decode()
    print(result)
    
    time.sleep(3)
    
    # 5. 检查作业状态
    print("\n=== 5. 检查作业状态 ===")
    stdin, stdout, stderr = ssh.exec_command("/opt/module/flink/bin/flink list")
    job_status = stdout.read().decode()
    print(job_status)
    
    if "RUNNING" in job_status:
        print("\n[OK] Flink作业已成功启动并运行！")
        print("\n等待3分钟让高频窗口触发，然后检查var_temp和kurtosis_temp...")
        
        # 等待窗口触发
        for i in range(3):
            time.sleep(60)
            print(f"  已等待 {i+1} 分钟...")
        
        # 检查Flink输出
        print("\n=== 6. 检查窗口计算结果 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "find /opt/module/flink/log -name '*.out' 2>/dev/null | head -1 | xargs tail -50 2>/dev/null"
        )
        flink_output = stdout.read().decode()
        if flink_output:
            # 查找包含var_temp的行
            import re
            var_lines = [l for l in flink_output.split('\n') if 'var_temp' in l or '高频窗口' in l]
            if var_lines:
                print("找到窗口计算结果:")
                for line in var_lines[-5:]:
                    print(f"  {line}")
            else:
                print("Flink输出日志:")
                print(flink_output[-1000:])
        else:
            print("未找到Flink输出日志")
    else:
        print("\n[WARN] 作业可能未正常运行")
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
