#!/bin/bash

# ============================================================
#  CNC ChangeRecord 实时模拟数据生成器 (服务器版)
#  基于 device_state.log / sensor_metrics.log 真实数据分布
#  持续运行，每 3 秒输出一条数据
#
#  输出文件:
#    device_state_generated.log  — 状态日志
#    sensor_metrics_generated.log — 传感器+统计数据
#
#  字段定义:
#    状态日志(7字段): machineId, duration_seconds, is_running,
#              is_standby, is_offline, isAlarm, cumulative_alarms
#    传感器数据(12字段): machineId, cutting_time, cycle_time,
#               spindle_load, feed_rate, spindle_speed,
#               total_parts, cumulative_runtime, cumulative_parts,
#               cumulative_alarms, avg_spindle_load_10(近10次均),
#               avg_cutting_time_10(近10次均)
#
#  传感器真实范围(来自生产数据):
#    spindle_load:  1 ~ 32
#    feed_rate:     35 ~ 33535
#    spindle_speed: 144 ~ 3200
#
#  用法:
#    bash generate_change_data.sh            # 默认每3秒一条
#    bash generate_change_data.sh 5          # 每5秒一条
#    nohup bash generate_change_data.sh &    # 后台运行
#    Ctrl+C 停止
# ============================================================

# --- 配置 ---
INTERVAL_SEC=${1:-3}
STATE_OUT="device_state_generated.log"
METRICS_OUT="sensor_metrics_generated.log"

# 9 台设备(来自真实数据)
MACHINES=(109 110 111 112 113 114 115 116 117)

# 真实数据中各设备的持续时长基准值(秒)
declare -A BASE_DURATION
BASE_DURATION[109]=772000; BASE_DURATION[110]=619000; BASE_DURATION[111]=788000
BASE_DURATION[112]=409000; BASE_DURATION[113]=0;       BASE_DURATION[114]=0
BASE_DURATION[115]=384000; BASE_DURATION[116]=795000; BASE_DURATION[117]=711000

# 各传感器基准值
declare -A BASE_CUTTING
BASE_CUTTING[109]=480000; BASE_CUTTING[110]=310000; BASE_CUTTING[111]=490000
BASE_CUTTING[112]=260000; BASE_CUTTING[113]=0;     BASE_CUTTING[114]=0
BASE_CUTTING[115]=225000; BASE_CUTTING[116]=518000; BASE_CUTTING[117]=372000

declare -A BASE_CYCLE
BASE_CYCLE[109]=820000; BASE_CYCLE[110]=650000; BASE_CYCLE[111]=840000
BASE_CYCLE[112]=550000; BASE_CYCLE[113]=0;     BASE_CYCLE[114]=0
BASE_CYCLE[115]=1000000;BASE_CYCLE[116]=574000; BASE_CYCLE[117]=1053000

declare -A BASE_TOTAL_PARTS
BASE_TOTAL_PARTS[109]=5200;  BASE_TOTAL_PARTS[110]=2800;  BASE_TOTAL_PARTS[111]=5500
BASE_TOTAL_PARTS[112]=4500;  BASE_TOTAL_PARTS[113]=0;     BASE_TOTAL_PARTS[114]=0
BASE_TOTAL_PARTS[115]=36000; BASE_TOTAL_PARTS[116]=7850;  BASE_TOTAL_PARTS[117]=65400

declare -A BASE_RUNTIME
BASE_RUNTIME[109]=1850000; BASE_RUNTIME[110]=1420000; BASE_RUNTIME[111]=1900000
BASE_RUNTIME[112]=920000;  BASE_RUNTIME[113]=0;      BASE_RUNTIME[114]=0
BASE_RUNTIME[115]=838000;  BASE_RUNTIME[116]=1799000;BASE_RUNTIME[117]=1328000

# ========== 错误码字典 ==========
ERROR_CODES=(
"0x01001" "0x01002" "0x01003" "0x01004" "0x01005"
"0x01006" "0x01007" "0x01008" "0x01009" "0x0100A"
"0x03001" "0x03002" "0x03003" "0x03004" "0x03005"
"0x03006" "0x03007" "0x03008" "0x03009" "0x0300A"
"0x0300B" "0x0300C" "0x0300D" "0x0300E" "0x0300F"
"0x05001" "0x05002" "0x05003" "0x05004" "0x05005"
"0x05006" "0x05007" "0x05008" "0x05009" "0x0500A"
"0x06001" "0x06002" "0x06003" "0x06004" "0x06005"
"0x06006" "0x06007" "0x06008" "0x06009" "0x0600A"
"0x12001" "0x12002" "0x12003" "0x12004" "0x12005"
"0x12006" "0x12007" "0x12008" "0x12009" "0x1200A"
"0x1200B" "0x1200C" "0x1200D" "0x1200E" "0x1200F"
"0x13001" "0x13002" "0x13003" "0x13004" "0x13005"
"0x13006" "0x13007" "0x13008" "0x13009" "0x1300A"
"0x16001" "0x16002" "0x16003" "0x16004" "0x16005"
"0x16006" "0x16007" "0x16008" "0x16009" "0x1600A"
"0x17001" "0x17002" "0x17003" "0x17004" "0x17005"
"0x17B01" "0x17B02" "0x17B03" "0x17B04" "0x17B05"
"0x000E1" "0x000E2" "0x000E3" "0x000E4" "0x000E5"
)
ERROR_COUNT=${#ERROR_CODES[@]}

# ========== 每台设备的状态变量 ==========
declare -A ACCUM_CUTTING      # 累计切削时间
declare -A ACCUM_CYCLE        # 累计循环时间
declare -A ACCUM_RUNTIME      # 累计运行时间
declare -A ACCUM_PARTS        # 累计加工个数
declare -A CUMULATIVE_ALARMS  # 累计报警次数（每出一次错误码+1）
declare -A LOAD_HISTORY       # 最近10次主轴负载（逗号分隔）
declare -A CUTTING_HISTORY    # 最近10次切削时间（逗号分隔）

for m in "${MACHINES[@]}"; do
    ACCUM_CUTTING[$m]=${BASE_CUTTING[$m]}
    ACCUM_CYCLE[$m]=${BASE_CYCLE[$m]}
    ACCUM_RUNTIME[$m]=${BASE_RUNTIME[$m]}
    ACCUM_PARTS[$m]=${BASE_TOTAL_PARTS[$m]}
    CUMULATIVE_ALARMS[$m]=0
    LOAD_HISTORY[$m]=""
    CUTTING_HISTORY[$m]=""
done

echo "============================================"
echo "  CNC ChangeRecord 实时模拟生成器 (服务器版)"
echo "============================================"
echo "  输出间隔: ${INTERVAL_SEC}秒/条"
echo "  状态日志: $STATE_OUT (追加模式)"
echo "  传感器数据: $METRICS_OUT (追加模式)"
echo "  错误码库: ${ERROR_COUNT} 个 | 概率 ~10%(约2条/分)"
echo "  传感器范围: load[1~32] feed[35~33535] speed[144~3200]"
echo "--------------------------------------------"
echo "  启动时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "  按 Ctrl+C 可随时停止"
echo "============================================"

count=0
run_count=0
standby_count=0
offline_count=0
error_count=0

# 随机浮点数
rand_float() {
    awk -v min="$1" -v max="$2" 'BEGIN{srand(); printf "%.1f", min+rand()*(max-min)}'
}

# 随机整数
rand_int() {
    awk -v min="$1" -v max="$2" 'BEGIN{srand(); printf "%d", int(min+rand()*(max-min+1))}'
}

# 滑动窗口计算：入队一个新值，返回 平均值|更新后的队列字符串
# 用法: result=$(push_window "$old_queue" "$new_value" 10)
push_window() {
    local queue="$1" val="$2" max_n=${3:-10}
    if [ -z "$queue" ]; then
        printf "%.1f|%s" "$val" "$val"
    else
        echo "${queue},${val}" | awk -v N="$max_n" -F',' '{
            n=split($0, a, ",");
            start=(n>N)?n-N+1:1;
            s=0; cnt=0;
            for(i=start;i<=n;i++){s+=a[i];cnt++}
            str=a[start];
            for(i=start+1;i<=n;i++){str=str","a[i]}
            printf "%.1f|%s", s/cnt, str
        }'
    fi
}

# 主循环（无限，Ctrl+C停止）
while true; do

    # 随机选一台设备
    idx=$(( RANDOM % ${#MACHINES[@]} ))
    mid=${MACHINES[$idx]}

    # 当前时间戳
    ts=$(date '+%Y-%m-%d %H:%M:%S')

    # ========== 状态概率模型 ==========
    # 分布: 运行50% | 待机47% | 离线3%
    rand_prob=$(( RANDOM % 100 ))

    if [ $rand_prob -lt 50 ]; then
        # ====== 运行状态 (50%) ======
        is_running=1
        is_standby=0
        is_offline=0
        run_count=$((run_count + 1))

        base_dur=${BASE_DURATION[$mid]:-100000}
        duration=$(awk -v b="$base_dur" 'BEGIN{printf "%.1f", b + rand()*7000 + 1000}')

        # 累计值递增（模拟设备在持续加工）
        inc_cutting=$(awk 'BEGIN{printf "%.1f", rand()*30+5}')
        inc_cycle=$(awk   'BEGIN{printf "%.1f", rand()*35+8}')
        inc_runtime=$(awk 'BEGIN{printf "%.1f", rand()*3+0.5}')
        inc_parts=$((RANDOM % 3))

        ACCUM_CUTTING[$mid]=$(awk -v v="${ACCUM_CUTTING[$mid]}" -v i="$inc_cutting" 'BEGIN{printf "%.1f", v+i}')
        ACCUM_CYCLE[$mid]=$(awk   -v v="${ACCUM_CYCLE[$mid]}"   -v i="$inc_cycle"   'BEGIN{printf "%.1f", v+i}')
        ACCUM_RUNTIME[$mid]=$(awk -v v="${ACCUM_RUNTIME[$mid]}" -v i="$inc_runtime" 'BEGIN{printf "%.1f", v+i}')
        ACCUM_PARTS[$mid]=$((${ACCUM_PARTS[$mid]} + inc_parts))

        cutting_time=${ACCUM_CUTTING[$mid]}
        cycle_time=${ACCUM_CYCLE[$mid]}

        # 传感器数值 — 运行状态：中高区间
        spindle_load=$(rand_float 12.0 32.0)       # 真实范围 1~32
        feed_rate=$(rand_int 500 33535)             # 真实范围 35~33535
        spindle_speed=$(rand_int 800 3200)          # 真实范围 144~3200

        total_parts=${ACCUM_PARTS[$mid]}
        cum_runtime=${ACCUM_RUNTIME[$mid]}
        cum_parts=$total_parts

    elif [ $rand_prob -lt 97 ]; then
        # ====== 待机状态 (47%) ======
        is_running=0
        is_standby=1
        is_offline=0
        standby_count=$((standby_count + 1))

        base_dur=${BASE_DURATION[$mid]:-50000}
        duration=$(awk -v b="$base_dur" 'BEGIN{printf "%.1f", b + rand()*5000 + 500}')

        cutting_time=${ACCUM_CUTTING[$mid]}
        cycle_time=${ACCUM_CYCLE[$mid]}

        # 传感器数值 — 待机状态：低区间
        spindle_load=$(rand_float 1.0 15.0)         # 真实范围 1~32
        feed_rate=$(rand_int 35 3000)               # 真实范围 35~33535
        spindle_speed=$(rand_int 144 1000)          # 真实范围 144~3200

        total_parts=${ACCUM_PARTS[$mid]}
        cum_runtime=${ACCUM_RUNTIME[$mid]}
        cum_parts=$total_parts

    else
        # ====== 离线状态 (3%) ======
        is_running=0
        is_standby=0
        is_offline=1
        offline_count=$((offline_count + 1))

        duration=0.0
        cutting_time=${ACCUM_CUTTING[$mid]}
        cycle_time=${ACCUM_CYCLE[$m]}

        # 传感器数值 — 离线状态：最低区间
        spindle_load=$(rand_float 1.0 8.0)          # 真实范围 1~32
        feed_rate=$(rand_int 35 500)                # 真实范围 35~33535
        spindle_speed=$(rand_int 144 400)           # 真实范围 144~3200

        total_parts=${ACCUM_PARTS[$mid]}
        cum_runtime=${ACCUM_RUNTIME[$mid]}
        cum_parts=$total_parts
    fi

    # ========== 错误码生成 (~10%概率) ==========
    error_code=""
    if [ $((RANDOM % 100)) -lt 10 ]; then
        err_idx=$(( RANDOM % ERROR_COUNT ))
        error_code="${ERROR_CODES[$err_idx]}"
        error_count=$((error_count + 1))
    fi

    # ========== 统计字段计算 ==========

    # 1) cumulative_alarms — 该设备每出一次错误码就+1
    if [ -n "$error_code" ]; then
        CUMULATIVE_ALARMS[$mid]=$((${CUMULATIVE_ALARMS[$mid]} + 1))
    fi
    cum_alarms=${CUMULATIVE_ALARMS[$mid]}

    # 2) avg_spindle_load_10 — 近10次平均主轴负载（滑动窗口）
    window_result=$(push_window "${LOAD_HISTORY[$mid]}" "$spindle_load" 10)
    avg_spindle_10=$(echo "$window_result" | cut -d'|' -f1)
    LOAD_HISTORY[$mid]=$(echo "$window_result" | cut -d'|' -f2)

    # 3) avg_cutting_time_10 — 近10次平均切削时间（滑动窗口）
    window_result=$(push_window "${CUTTING_HISTORY[$mid]}" "$cutting_time" 10)
    avg_cutting_10=$(echo "$window_result" | cut -d'|' -f1)
    CUTTING_HISTORY[$mid]=$(echo "$window_result" | cut -d'|' -f2)

    # ========== 输出状态日志 (7字段) ==========
    state_json=$(printf '{"machineId":%s,"duration_seconds":%s,"is_running":%d,"is_standby":%d,"is_offline":%d,"isAlarm":"%s","cumulative_alarms":%d}' \
        "$mid" "$duration" "$is_running" "$is_standby" "$is_offline" "$error_code" "$cum_alarms")
    echo "$state_json" >> "$STATE_OUT"

    # ========== 输出传感器数据 (12字段: machineId + 11个传感器/统计字段) ==========
    metrics_json=$(printf '{"machineId":%s,"cutting_time":%s,"cycle_time":%s,"spindle_load":%.1f,"feed_rate":%d,"spindle_speed":%d,"total_parts":%d,"cumulative_runtime":%s,"cumulative_parts":%d,"cumulative_alarms":%d,"avg_spindle_load_10":%.1f,"avg_cutting_time_10":%.1f}' \
        "$mid" "$cutting_time" "$cycle_time" "$spindle_load" "$feed_rate" "$spindle_speed" "$total_parts" "$cum_runtime" "$cum_parts" "$cum_alarms" "$avg_spindle_10" "$avg_cutting_10")
    echo "$metrics_json" >> "$METRICS_OUT"

    count=$((count + 1))

    # 每10条打印一次进度
    if [ $((count % 10)) -eq 0 ]; then
        echo "  [$ts] 第${count}条 | 设备:$mid | 运行:$run_count 待机:$standby_count 离线:$offline_count 错误:$error_count"
    fi

    sleep "$INTERVAL_SEC"

done
