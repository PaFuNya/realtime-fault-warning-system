#!/usr/bin/env python3
import paramiko
import sys
import time

def quick_check():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!")
        
        # 等待3分钟
        print("\n等待3分钟让高频窗口触发...")
        for i in range(6):
            time.sleep(30)
            print(f"  已等待 {(i+1)*30} 秒")
        
        # 检查TaskManager日志
        print("\n=== 检查高频窗口计算结果 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cat /opt/module/flink/log/flink-*-taskexecutor-*.out 2>/dev/null | grep -E 'var_temp|kurtosis' | tail -10"
        )
        logs = stdout.read().decode().strip()
        
        if logs:
            print("计算结果:")
            print(logs)
            
            # 统计非零值
            import re
            var_values = re.findall(r'var_temp=([0-9.]+)', logs)
            nonzero = [v for v in var_values if float(v) > 0]
            print(f"\n统计: 共{len(var_values)}条var_temp记录，其中{len(nonzero)}条 > 0")
            
            if len(nonzero) > 0:
                print("\n[成功] var_temp 已正常计算！")
            else:
                print("\n[注意] var_temp 仍为0，可能需要检查其他问题")
        else:
            print("暂无计算结果")
        
        # 检查作业状态
        print("\n=== 作业状态 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cd /opt/module/flink && bin/flink list -Drest.port=8090 2>&1 | grep RUNNING"
        )
        print(stdout.read().decode().strip())
        
        ssh.close()
        print("\n=== 检查完成 ===")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    quick_check()
