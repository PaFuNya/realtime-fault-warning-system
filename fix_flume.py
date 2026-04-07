#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复Flume缺少Guava库的问题
"""

import paramiko

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    print("=== 连接到服务器修复Flume ===")
    ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
    
    # 1. 检查Guava库是否存在
    print("\n=== 1. 检查Guava库 ===")
    stdin, stdout, stderr = ssh.exec_command("find /opt -name 'guava*.jar' 2>/dev/null | head -5")
    guava_jars = stdout.read().decode().strip()
    if guava_jars:
        print(f"找到Guava库:\n{guava_jars}")
    else:
        print("未找到Guava库，需要下载")
    
    # 2. 检查Flume lib目录
    print("\n=== 2. 检查Flume lib目录 ===")
    stdin, stdout, stderr = ssh.exec_command("ls -la /opt/module/flume/lib/ | grep guava")
    result = stdout.read().decode().strip()
    if result:
        print(f"Flume lib中的Guava: {result}")
    else:
        print("Flume lib目录中没有Guava库")
    
    # 3. 查找其他位置的Guava
    print("\n=== 3. 在其他组件中查找Guava ===")
    stdin, stdout, stderr = ssh.exec_command("find /opt/module -name 'guava*.jar' 2>/dev/null")
    all_guava = stdout.read().decode().strip()
    if all_guava:
        print(f"找到Guava库:\n{all_guava}")
        
        # 复制第一个找到的Guava到Flume lib
        first_guava = all_guava.split('\n')[0]
        print(f"\n复制 {first_guava} 到 /opt/module/flume/lib/")
        stdin, stdout, stderr = ssh.exec_command(f"cp {first_guava} /opt/module/flume/lib/")
        
        # 验证
        stdin, stdout, stderr = ssh.exec_command("ls -la /opt/module/flume/lib/guava*")
        print(stdout.read().decode())
    else:
        print("未找到Guava库，需要从Maven仓库下载")
        # 下载Guava
        print("\n下载Guava 27.0.1-jre...")
        stdin, stdout, stderr = ssh.exec_command(
            "cd /opt/module/flume/lib && wget -q https://repo1.maven.org/maven2/com/google/guava/guava/27.0.1-jre/guava-27.0.1-jre.jar"
        )
        stdin, stdout, stderr = ssh.exec_command("ls -la /opt/module/flume/lib/guava*")
        print(stdout.read().decode())
    
    # 4. 检查是否还需要其他依赖
    print("\n=== 4. 检查其他可能缺失的依赖 ===")
    stdin, stdout, stderr = ssh.exec_command("ls -la /opt/module/flume/lib/ | wc -l")
    lib_count = stdout.read().decode().strip()
    print(f"Flume lib目录中有 {lib_count} 个文件")
    
    # 5. 重新启动Flume
    print("\n=== 5. 重新启动Flume ===")
    # 先停止可能存在的进程
    ssh.exec_command("pkill -f flume-ng")
    import time
    time.sleep(2)
    
    # 启动Flume
    flume_home = "/opt/module/flume"
    config_file = "/opt/module/flume/conf/MyFlume.conf"
    cmd = f"cd {flume_home} && nohup bin/flume-ng agent --conf conf --conf-file {config_file} --name a1 -Dflume.root.logger=INFO,LOGFILE > /var/log/flume-agent.log 2>&1 &"
    print(f"执行: {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    time.sleep(5)
    
    # 检查是否启动成功
    stdin, stdout, stderr = ssh.exec_command("ps aux | grep flume-ng | grep -v grep | wc -l")
    count = int(stdout.read().decode().strip() or 0)
    if count > 0:
        print(f"[OK] Flume启动成功，{count}个进程在运行")
        
        # 显示日志
        print("\n=== 6. Flume日志 ===")
        time.sleep(3)
        stdin, stdout, stderr = ssh.exec_command("tail -50 /var/log/flume-agent.log")
        log = stdout.read().decode()
        print(log[-1500:] if len(log) > 1500 else log)
    else:
        print("[FAIL] Flume启动失败")
        stdin, stdout, stderr = ssh.exec_command("tail -50 /var/log/flume-agent.log")
        print(stdout.read().decode())
    
    ssh.close()
    print("\n=== 完成 ===")

if __name__ == '__main__':
    main()
