import json
import time
import random
import threading
import collections
from kafka import KafkaProducer
import matplotlib.pyplot as plt
import matplotlib.animation as animation

# 1. 配置 Kafka 连接 (建议检查 IP 是否能 ping 通)
BOOTSTRAP_SERVERS = [
    '100.126.226.67:9092',
    '100.90.72.128:9092',
    '100.123.80.25:9092'
]

def get_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1, # 只要 leader 确认收到即可，提高速度
            retries=3
        )
    except Exception as e:
        print(f"❌ Kafka 连接失败: {e}")
        exit(1)

def generate_sensor_data(machine_id):
    """生成 sensor_raw 数据，确保 ts 是当前毫秒时间戳"""
    return {
        "machine_id": machine_id,
        "ts": int(time.time() * 1000), # 核心：使用当前时间
        "temperature": round(random.uniform(40.0, 95.0), 2), # 稍微调高点，容易出 WARNING
        "vibration_x": round(random.uniform(0.1, 5.0), 3),
        "current": round(random.uniform(10.0, 50.0), 2)
    }

# --- 可视化类 ---
class LivePlot:
    def __init__(self, machine_id_to_plot='Machine_001'):
        self.machine_id = machine_id_to_plot
        self.temp_data = collections.deque(maxlen=100)
        self.timestamps = collections.deque(maxlen=100)
        self.fig, self.ax = plt.subplots()
        self.line, = self.ax.plot([], [], 'r-')
        self.ax.set_title(f"Real-time Temperature: {self.machine_id}")
        self.ax.set_ylabel("Temp (°C)")
        self.start_time = time.time()

    def update(self, new_temp, new_ts):
        rel_time = (new_ts / 1000.0) - self.start_time
        self.temp_data.append(new_temp)
        self.timestamps.append(rel_time)

    def animate(self, i):
        if len(self.timestamps) > 1:
            self.line.set_data(list(self.timestamps), list(self.temp_data))
            self.ax.set_xlim(min(self.timestamps), max(self.timestamps) + 2)
            self.ax.set_ylim(min(self.temp_data) - 5, max(self.temp_data) + 5)
        return self.line,

    def show(self):
        self.ani = animation.FuncAnimation(self.fig, self.animate, interval=500, blit=False)
        plt.show()

# --- 主逻辑 ---
def main():
    producer = get_producer()
    machine_ids = [f"Machine_{i:03d}" for i in range(1, 6)]
    my_plot = LivePlot(machine_id_to_plot='Machine_001')

    print("🚀 脚本已启动！正在往 Kafka 发送数据...")

    def send_data_thread():
        count = 0
        while True:
            for mid in machine_ids:
                data = generate_sensor_data(mid)
                producer.send('sensor_raw', data)

                if mid == my_plot.machine_id:
                    my_plot.update(data['temperature'], data['ts'])

            count += 1
            if count % 20 == 0:
                print(f"📈 已发送 {count * len(machine_ids)} 条数据，最新时间戳: {int(time.time()*1000)}")

            # 缩短间隔到 0.2 秒，加快窗口触发速度
            time.sleep(0.2)

    t = threading.Thread(target=send_data_thread, daemon=True)
    t.start()

    my_plot.show()
    producer.close()

if __name__ == "__main__":
    main()