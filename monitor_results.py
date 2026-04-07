#!/usr/bin/env python3
import paramiko
import sys
import time

def monitor_results():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        print("正在连接 192.168.45.100...")
        ssh.connect('192.168.45.100', username='root', password='123456', timeout=10)
        print("连接成功!")
        print("\n=== 监控 Flink 高频窗口计算结果 ===")
        print("窗口大小: 3分钟")
        print("预计首次窗口触发: 3分钟后\n")
        
        # 清空旧日志以便观察新结果
        print("=== 清空旧TaskManager日志 ===")
        ssh.exec_command("> /opt/module/flink/log/flink-*-taskexecutor-*.out 2>/dev/null")
        
        found_nonzero = False
        
        for i in range(15):  # 监控约7.5分钟
            elapsed = (i + 1) * 30
            print(f"\n--- 已等待 {elapsed} 秒 ---")
            
            # 检查TaskManager日志
            stdin, stdout, stderr = ssh.exec_command(
                "cat /opt/module/flink/log/flink-*-taskexecutor-*.out 2>/dev/null"
            )
            logs = stdout.read().decode().strip()
            
            if logs:
                # 查找高频窗口相关日志
                highfreq_logs = []
                for line in logs.split('\n'):
                    if '高频窗口' in line or 'var_temp' in line or 'kurtosis' in line:
                        highfreq_logs.append(line)
                
                if highfreq_logs:
                    print("高频窗口计算结果:")
                    for log in highfreq_logs[-5:]:
                        print(f"  {log}")
                        # 检查是否有非零值
                        if 'var_temp=' in log:
                            try:
                                import re
                                var_match = re.search(r'var_temp=([0-9.]+)', log)
                                if var_match:
                                    var_val = float(var_match.group(1))
                                    if var_val > 0:
                                        found_nonzero = True
                                        print(f"  *** 发现非零var_temp: {var_val} ***")
                            except:
                                pass
                
                # 查找合并日志
                merge_logs = [line for line in logs.split('\n') if '合并' in line]
                if merge_logs:
                    print("合并日志:")
                    for log in merge_logs[-3:]:
                        print(f"  {log}")
            
            # 检查作业状态
            stdin, stdout, stderr = ssh.exec_command(
                "cd /opt/module/flink && bin/flink list -Drest.port=8090 2>&1 | grep -E 'RUNNING|JobID'"
            )
            status = stdout.read().decode().strip()
            if status:
                print(f"作业状态: {status}")
            
            if found_nonzero and i > 6:  # 找到非零值后再等一会儿就退出
                print("\n*** 已成功计算出非零var_temp! ***")
                break
            
            if i < 14:
                time.sleep(30)
        
        # 最终检查
        print("\n\n=== 最终结果 ===")
        stdin, stdout, stderr = ssh.exec_command(
            "cat /opt/module/flink/log/flink-*-taskexecutor-*.out 2>/dev/null | grep -E 'var_temp|kurtosis_temp' | tail -10"
        )
        final_results = stdout.read().decode().strip()
        if final_results:
            print("var_temp 和 kurtosis_temp 计算结果:")
            print(final_results)
            
            # 统计非零值
            import re
            var_values = re.findall(r'var_temp=([0-9.]+)', final_results)
            nonzero_count = sum(1 for v in var_values if float(v) > 0)
            print(f"\n统计: 共{len(var_values)}条记录，其中{nonzero_count}条var_temp > 0")
            
            if nonzero_count > 0:
                print("\n[成功] var_temp 和 kurtosis_temp 已正常计算！")
            else:
                print("\n[注意] 所有var_temp仍为0，可能需要更长时间或检查其他问题")
        else:
            print("暂无计算结果")
        
        ssh.close()
        print("\n=== 监控完成 ===")
        
    except Exception as e:
        print(f"操作失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    monitor_results()
