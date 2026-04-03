#!/bin/bash

# Configuration
SENSOR_LOG_FILE="sensor_raw.log"
LOG_RAW_FILE="log_raw.log"

# 如果你确定设备 ID 就是 101, 102, 103, 104, 105, 112, 114, 115, 116 等等
# 我们直接在这里把可能的 ID 写死，这样就不需要依赖外部传参了！
MACHINES=("101" "102" "103" "104" "105" "112" "114" "115" "116" "117" "118")

# Ensure files exist and are empty at start (optional)
> "$SENSOR_LOG_FILE"
> "$LOG_RAW_FILE"

echo "🚀 开始持续生成模拟数据到 $SENSOR_LOG_FILE 和 $LOG_RAW_FILE ..."
echo "💡 (提示：按下 Ctrl+C 即可安全停止脚本)"

count=0

# Helper function to generate a random float between two numbers
# Usage: rand_float min max
rand_float() {
    local min=$1
    local max=$2
    awk -v min="$min" -v max="$max" 'BEGIN{srand(); print min+rand()*(max-min)}'
}

# Instead of looping through all 5 machines at once, we pick ONE random machine per iteration
while true; do
    # Get current timestamp in milliseconds
    ts=$(date +%s%3N)
    
    # Pick one random machine from the array
    mid=${MACHINES[$((RANDOM % ${#MACHINES[@]}))]}

    # ---------------------------------------------------------
    # 1. Generate Sensor Data
        # ---------------------------------------------------------
        # Simulate anomaly (5% chance)
        is_anomaly=0
        if [ $((RANDOM % 100)) -lt 5 ]; then
            is_anomaly=1
        fi

        if [ $is_anomaly -eq 1 ]; then
            temp=$(rand_float 85.0 95.0)
            vib_mult=1.5
            noise=$(rand_float 80.0 90.0)
        else
            temp=$(rand_float 58.0 62.0)
            vib_mult=1.0
            noise=$(rand_float 50.0 60.0)
        fi

        vib_x=$(awk -v m="$vib_mult" -v r="$(rand_float 0.9 1.1)" 'BEGIN{printf "%.3f", m*r}')
        vib_y=$(awk -v m="$vib_mult" -v r="$(rand_float 0.9 1.1)" 'BEGIN{printf "%.3f", m*r}')
        vib_z=$(awk -v m="$vib_mult" -v r="$(rand_float 0.9 1.1)" 'BEGIN{printf "%.3f", m*r}')
        
        current=$(rand_float 29.0 31.0)
        
        # 增加频繁触发状态更改：20% 的概率 speed < 1000 (触发启动中/异常停机逻辑)
        if [ $((RANDOM % 100)) -lt 20 ]; then
            speed=$(rand_float 500.0 999.0)
        else
            speed=$(rand_float 2900.0 3100.0)
        fi

        # Format sensor JSON
        sensor_json=$(printf '{"machine_id": "%s", "ts": %s, "temperature": %.2f, "vibration_x": %s, "vibration_y": %s, "vibration_z": %s, "current": %.2f, "noise": %.2f, "speed": %.2f}' \
            "$mid" "$ts" "$temp" "$vib_x" "$vib_y" "$vib_z" "$current" "$noise" "$speed")
        
        echo "$sensor_json" >> "$SENSOR_LOG_FILE"

        # ---------------------------------------------------------
        # 2. Generate Log Data (30% chance per machine per iteration)
        # ---------------------------------------------------------
        if [ $((RANDOM % 10)) -lt 3 ]; then
            rand_val=$((RANDOM % 100))
            if [ $rand_val -lt 60 ]; then
                # 60% Normal
                err_code="200"
                err_msg="系统运行正常"
                stack=""
            elif [ $rand_val -lt 70 ]; then
                # 10% Warning
                err_code="500"
                err_msg="数据处理时发生内部错误"
                stack="java.lang.NullPointerException\n\tat org.example.Process.run(Process.java:42)"
            else
                # 30% Critical (提升 999 报错的出现频率，大概占总体日志生成的 10% 左右)
                err_code="999"
                err_msg="检测到严重硬件故障"
                stack="HardwareException: Sensor unresponsive\n\tat driver.Hardware.read(Hardware.c:120)"
            fi

            # Format log JSON
            log_json=$(printf '{"machine_id": "%s", "ts": %s, "error_code": "%s", "error_msg": "%s", "stack_trace": "%s"}' \
                "$mid" "$ts" "$err_code" "$err_msg" "$stack")
            
            echo "$log_json" >> "$LOG_RAW_FILE"
        fi

    count=$((count + 1))
    if [ $((count % 10)) -eq 0 ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - INFO - 📈 已生成 $count 条数据，当前运行正常..."
    fi

    # 跑快一些：从 1 秒改为 0.3 秒一条数据，一秒 3-4 条，适中且比之前快
    sleep 0.3
done