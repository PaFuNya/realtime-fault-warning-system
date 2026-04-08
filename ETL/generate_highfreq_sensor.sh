#!/bin/bash

# ============================================================
#  CNC 高频传感器时序数据生成器 v2 (高性能版)
#
#  v2 改进: 用单次 awk 批量生成所有9台设备的数据
#        每轮循环只调用1次 awk (原版: ~180次)
#        性能提升: ~20x, 不再卡顿
#
#  输出文件: highfreq_sensor_generated.log
#  Kafka:    highfreq_sensor topic (后台异步发送)
#
#  用法:
#    bash generate_highfreq_sensor.sh          # 默认每1秒一轮(9条)
#    bash generate_highfreq_sensor.sh 0.5      # 每0.5秒一轮
#    nohup bash generate_highfreq_sensor.sh &  # 后台运行
# ============================================================

set -u

# --- 配置 ---
INTERVAL_SEC=${1:-1}
OUT_FILE="highfreq_sensor_generated.log"
KAFKA_TOPIC="highfreq_sensor"
KAFKA_BROKER="master:9092"

echo "============================================"
echo "  CNC 高频传感器时序数据生成器 v2 (高性能)"
echo "============================================"
echo "  输出间隔: ${INTERVAL_SEC}秒/轮(9条)"
echo "  输出文件: $OUT_FILE"
echo "  启动时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "  按 Ctrl+C 可随时停止"
echo "============================================"

count=0

# ======== 主循环 ========
while true; do

    # 获取当前时间戳
    ts=$(date '+%Y-%m-%d %H:%M:%S')

    # ========== 单次 awk 调用，批量生成9台设备的全部数据 ==========
    # 核心思路: 把所有随机数生成、状态机、漂移、边界检查全部在 awk 内完成
    # 只输出最终JSON行到 stdout, 再由 shell 写入文件
    json_block=$(
    awk '
    BEGIN {
        srand()

        # 设备列表
        split("109 110 111 112 113 114 115 116 117", machines)

        # 各设备基准值
        base_temp[109]=45; base_temp[110]=42; base_temp[111]=48
        base_temp[112]=40; base_temp[113]=38; base_temp[114]=41
        base_temp[115]=50; base_temp[116]=47; base_temp[117]=44

        base_vib[109]=2.0; base_vib[110]=1.8; base_vib[111]=2.2
        base_vib[112]=1.5; base_vib[113]=1.2; base_vib[114]=1.4
        base_vib[115]=2.8; base_vib[116]=2.5; base_vib[117]=1.9

        base_load[109]=22; base_load[110]=18; base_load[111]=24
        base_load[112]=16; base_load[113]=10; base_load[114]=12
        base_load[115]=26; base_load[116]=20; base_load[117]=19

        base_feed[109]=12000; base_feed[110]=8000;  base_feed[111]=15000
        base_feed[112]=6000;  base_feed[113]=4000;  base_feed[114]=5000
        base_feed[115]=20000; base_feed[116]=10000; base_feed[117]=9000

        base_speed[109]=2200; base_speed[110]=1800; base_speed[111]=2400
        base_speed[112]=1500; base_speed[113]=1200; base_speed[114]=1400
        base_speed[115]=2800; base_speed[116]=2000; base_speed[117]=1900

        ts = ARGV[1]
        delete ARGV[1]

        for (i = 1; i <= 9; i++) {
            mid = machines[i]

            # --- 状态决定 (~3.3%概率切换) ---
            if (int(rand() * 30) == 0) {
                r = int(rand() * 100)
                if (r < 55) dev_state[mid] = 2       # 运行 55%
                else if (r < 95) dev_state[mid] = 1   # 待机 40%
                else dev_state[mid] = 0                 # 离线 5%
            }
            if (!(mid in dev_state)) dev_state[mid] = 2
            state = dev_state[mid]

            # --- 温度漂移 ---
            if (int(rand() * 100) < 3) {
                temp_drift[mid] += rand() * 2 - 0.5
                if (temp_drift[mid] < -5) temp_drift[mid] = -5
                if (temp_drift[mid] > 15) temp_drift[mid] = 15
            }
            if (!(mid in temp_drift)) temp_drift[mid] = 0

            if (state == 2) {
                # ====== 运行状态 ======
                is_running = 1
                bt = base_temp[mid] + temp_drift[mid]
                # 正态近似: 均值bt, 标准差4
                temperature = bt + (rand() + rand() + rand() - 1.5) * 4.0
                if (temperature < 25) temperature = 25
                if (temperature > 80) temperature = 80

                bv = base_vib[mid]
                vibration_x = bv + (rand() + rand() + rand() - 1.5) * 0.5
                vibration_y = bv * 0.8 + (rand() + rand() + rand() - 1.5) * 0.3
                vibration_z = bv * 1.2 + (rand() + rand() + rand() - 1.5) * 0.6
                if (vibration_x < 0.05) vibration_x = 0.05
                if (vibration_x > 8.0) vibration_x = 8.0
                if (vibration_y < 0.03) vibration_y = 0.03
                if (vibration_y > 6.0) vibration_y = 6.0
                if (vibration_z < 0.08) vibration_z = 0.08
                if (vibration_z > 10.0) vibration_z = 10.0

                bl = base_load[mid]
                spindle_load = bl - 5 + rand() * (bl + 10 - (bl - 5))
                if (spindle_load < 8) spindle_load = 8
                if (spindle_load > 32) spindle_load = 32

                bf = base_feed[mid]
                feed_rate = int(bf - 3000 + rand() * 8000)
                if (feed_rate < 200) feed_rate = 200
                if (feed_rate > 33535) feed_rate = 33535

                bs = base_speed[mid]
                spindle_speed = int(bs - 400 + rand() * 1000)
                if (spindle_speed < 500) spindle_speed = 500
                if (spindle_speed > 3200) spindle_speed = 3200

            } else if (state == 1) {
                # ====== 待机状态 ======
                is_running = 0
                temperature = 25.0 + rand() * 13.0
                vibration_x = 0.01 + rand() * 0.29
                vibration_y = 0.01 + rand() * 0.19
                vibration_z = 0.02 + rand() * 0.48
                spindle_load = 1.0 + rand() * 7.0
                feed_rate = int(35 + rand() * 765)
                spindle_speed = int(144 + rand() * 456)

            } else {
                # ====== 离线状态 ======
                is_running = 0
                temperature = 18.0 + rand() * 10.0
                vibration_x = rand() * 0.05
                vibration_y = rand() * 0.04
                vibration_z = rand() * 0.08
                spindle_load = rand() * 2.0
                feed_rate = 0
                spindle_speed = 0
            }

            # --- 异常尖峰 (运行状态3%概率) ---
            if (state == 2 && int(rand() * 100) < 3) {
                atype = int(rand() * 4)
                if (atype == 0) {
                    temperature += rand() * 15 + 5
                } else if (atype == 1) {
                    vibration_x += rand() * 3 + 1
                    vibration_z += rand() * 1.5 + 0.5
                } else if (atype == 2) {
                    spindle_load += rand() * 8 + 3
                    if (spindle_load > 32) spindle_load = 32
                } else {
                    temperature += rand() * 8 + 2
                    vibration_x += rand() * 1 + 0.3
                    spindle_load += rand() * 4 + 1
                    if (spindle_load > 32) spindle_load = 32
                }
            }

            # --- 最终边界检查 ---
            if (temperature < 15) temperature = 15
            if (temperature > 95) temperature = 95
            if (vibration_x < 0) vibration_x = 0
            if (vibration_x > 12) vibration_x = 12
            if (vibration_y < 0) vibration_y = 0
            if (vibration_y > 10) vibration_y = 10
            if (vibration_z < 0) vibration_z = 0
            if (vibration_z > 15) vibration_z = 15

            # --- 输出JSON ---
            printf "{\"machineId\":%s,\"timestamp\":\"%s\",\"temperature\":%.1f,\"vibration_x\":%.3f,\"vibration_y\":%.3f,\"vibration_z\":%.3f,\"spindle_load\":%.1f,\"feed_rate\":%d,\"spindle_speed\":%d,\"is_running\":%d}\n",
                mid, ts, temperature, vibration_x, vibration_y, vibration_z,
                spindle_load, feed_rate, spindle_speed, is_running
        }
    }
    ' "$ts"
    )

    # 批量写入本地日志文件 (只做1次I/O!)
    echo "$json_block" >> "$OUT_FILE"

    # 统计行数
    count=$((count + 9))

    # 进度显示
    if [ $((count % 90)) -eq 0 ]; then
        echo "  [$ts] 已输出 ${count} 条高频记录 | 文件: $OUT_FILE"
    fi

    # Kafka 异步发送 (后台, 不阻塞主循环)
    # 用括号开 subshell 避免变量污染
    (
        # 检查 kafka 命令是否存在且 Kafka 是否可用 (快速判断)
        if command -v kafka-console-producer.sh >/dev/null 2>&1; then
            echo "$json_block" | timeout 5 kafka-console-producer.sh \
                --broker-list "$KAFKA_BROKER" \
                --topic "$KAFKA_TOPIC" \
                2>/dev/null
        fi
    ) &

    sleep "$INTERVAL_SEC"

done
