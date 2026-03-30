#!/bin/bash

# Configuration
SENSOR_LOG_FILE="sensor_raw.log"
LOG_RAW_FILE="log_raw.log"
MACHINES=("Machine_001" "Machine_002" "Machine_003" "Machine_004" "Machine_005")

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

while true; do
    # Get current timestamp in milliseconds
    ts=$(date +%s%3N)
    
    for mid in "${MACHINES[@]}"; do
        # ---------------------------------------------------------
        # 1. Generate Sensor Data
        # ---------------------------------------------------------
        # Simulate anomaly (0.1% chance)
        is_anomaly=0
        if [ $((RANDOM % 1000)) -eq 0 ]; then
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
        speed=$(rand_float 2900.0 3100.0)

        # Format sensor JSON
        sensor_json=$(printf '{"machine_id": "%s", "ts": %s, "temperature": %.2f, "vibration_x": %s, "vibration_y": %s, "vibration_z": %s, "current": %.2f, "noise": %.2f, "speed": %.2f}' \
            "$mid" "$ts" "$temp" "$vib_x" "$vib_y" "$vib_z" "$current" "$noise" "$speed")
        
        echo "$sensor_json" >> "$SENSOR_LOG_FILE"

        # ---------------------------------------------------------
        # 2. Generate Log Data (10% chance per machine per iteration)
        # ---------------------------------------------------------
        if [ $((RANDOM % 10)) -eq 0 ]; then
            rand_val=$((RANDOM % 1000))
            if [ $rand_val -lt 980 ]; then
                # 98% Normal
                err_code="200"
                err_msg="System running normally"
                stack=""
            elif [ $rand_val -lt 995 ]; then
                # 1.5% Warning
                err_code="500"
                err_msg="Internal server error during data processing"
                stack="java.lang.NullPointerException\n\tat org.example.Process.run(Process.java:42)"
            else
                # 0.5% Critical
                err_code="999"
                err_msg="Critical hardware failure detected"
                stack="HardwareException: Sensor unresponsive\n\tat driver.Hardware.read(Hardware.c:120)"
            fi

            # Format log JSON
            log_json=$(printf '{"machine_id": "%s", "ts": %s, "error_code": "%s", "error_msg": "%s", "stack_trace": "%s"}' \
                "$mid" "$ts" "$err_code" "$err_msg" "$stack")
            
            echo "$log_json" >> "$LOG_RAW_FILE"
        fi
    done

    count=$((count + 1))
    if [ $((count % 10)) -eq 0 ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - INFO - 📈 已循环生成 $count 批次数据，当前运行正常..."
    fi

    # Sleep 0.5 seconds to simulate real-time stream
    sleep 0.5
done