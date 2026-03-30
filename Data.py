import json
import time
import random
import logging
import signal
import sys
import math
import os

# 1. 配置日志格式
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 用于控制循环的全局标志
running = True

# 数据文件输出路径
SENSOR_LOG_FILE = "sensor_raw.log"
DEVICE_LOG_FILE = "log_raw.log"

def signal_handler(sig, frame):
    """处理 Ctrl+C 退出信号，保证脚本优雅退出"""
    global running
    logging.info("接收到停止信号(Ctrl+C)，准备优雅退出...")
    running = False

# 注册信号处理器
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def generate_sensor_data(machine_id):
    """
    生成物联网传感器数据 (sensor_raw)
    包含字段: 设备 ID, 时间戳, 温度, 振动 (X/Y/Z), 电流, 噪声, 转速.
    """
    # 模拟真实数据，降低异常发生概率到 0.1%
    is_anomaly = random.random() < 0.001 
    
    # 基础正弦波周期变化模拟真实设备运转
    base_temp = 60.0 + 5.0 * math.sin(time.time() / 60.0)
    base_vib = 1.0 + 0.2 * math.sin(time.time() / 10.0)
    base_current = 30.0 + 2.0 * math.cos(time.time() / 30.0)
    
    temperature = random.uniform(85.0, 95.0) if is_anomaly else random.uniform(base_temp - 2.0, base_temp + 2.0)
    
    # 震动突增模拟
    vib_multiplier = 1.5 if is_anomaly else 1.0
    
    return {
        "machine_id": machine_id,
        "ts": int(time.time() * 1000),
        "temperature": round(temperature, 2),
        "vibration_x": round(random.uniform(base_vib - 0.1, base_vib + 0.1) * vib_multiplier, 3),
        "vibration_y": round(random.uniform(base_vib - 0.1, base_vib + 0.1) * vib_multiplier, 3),
        "vibration_z": round(random.uniform(base_vib - 0.1, base_vib + 0.1) * vib_multiplier, 3),
        "current": round(random.uniform(base_current - 1.0, base_current + 1.0), 2),
        "noise": round(random.uniform(50.0, 60.0) if not is_anomaly else random.uniform(80.0, 90.0), 2),
        "speed": round(random.uniform(2900.0, 3100.0), 2)
    }

def generate_log_data(machine_id):
    """
    生成设备运行日志数据 (log_raw)
    包含字段: 设备 ID, 时间戳, 报错代码, 错误信息, 堆栈轨迹.
    """
    rand_val = random.random()
    # 模拟日志分布：98% 正常(200), 1.5% 业务报错(500), 0.5% 严重故障(999)
    if rand_val < 0.98:
        error_code = "200"
        error_msg = "System running normally"
        stack_trace = ""
    elif rand_val < 0.995:
        error_code = "500"
        error_msg = "Internal server error during data processing"
        stack_trace = "java.lang.NullPointerException\\n\\tat org.example.Process.run(Process.java:42)"
    else:
        error_code = "999"
        error_msg = "Critical hardware failure detected"
        stack_trace = "HardwareException: Sensor unresponsive\\n\\tat driver.Hardware.read(Hardware.c:120)"

    return {
        "machine_id": machine_id,
        "ts": int(time.time() * 1000),
        "error_code": error_code,
        "error_msg": error_msg,
        "stack_trace": stack_trace
    }

def main():
    machine_ids = [f"Machine_{i:03d}" for i in range(1, 6)]
    
    logging.info(f"🚀 开始持续生成模拟数据到 {SENSOR_LOG_FILE} 和 {DEVICE_LOG_FILE} ...")
    logging.info("💡 (提示：按下 Ctrl+C 即可安全停止脚本)")
    
    count = 0
    try:
        # 使用追加模式打开文件
        with open(SENSOR_LOG_FILE, 'a', encoding='utf-8') as sensor_file, \
             open(DEVICE_LOG_FILE, 'a', encoding='utf-8') as log_file:
             
            while running:
                for mid in machine_ids:
                    # 1. 每次循环都生成一条传感器数据并写入文件
                    sensor_data = generate_sensor_data(mid)
                    sensor_file.write(json.dumps(sensor_data) + '\n')
                    
                    # 2. 模拟日志不是每秒都有，设定 10% 的概率产生一条日志
                    if random.random() < 0.1:
                        log_data = generate_log_data(mid)
                        log_file.write(json.dumps(log_data) + '\n')
                        
                # 刷新缓冲区，确保数据实时写入磁盘供 Flume 采集
                sensor_file.flush()
                log_file.flush()
                        
                count += 1
                if count % 10 == 0:
                    logging.info(f"📈 已循环生成 {count} 批次数据，当前运行正常...")
                
                # 间隔 0.5 秒，模拟真实的发送频率
                time.sleep(0.5)
                
    except Exception as e:
        logging.error(f"⚠️ 运行过程中发生异常: {e}")
    finally:
        logging.info("👋 脚本已安全退出。")

if __name__ == "__main__":
    main()
