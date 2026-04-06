#!/bin/bash

# ============================================================
#  CNC 高频传感器时序数据生成器 (服务器版)
#
#  模拟 PLC/边缘网关采集的高频时序数据源
#  用于 Flink 窗口聚合计算以下 7 个时序特征:
#    1. var_temp        — 温度波动方差 (VAR_POP)
#    2. kurtosis_temp   — 温度峭度 (冲击性检测)
#    3. fft_peak        — 振动频域峰值 (FFT主频率)
#    4. avg_spindle_load— 窗口平均主轴负载
#    5. max_spindle_load— 窗口峰值主轴负载
#    6. running_ratio   — 设备运行占比
#    7. avg_feed_rate   — 窗口平均进给速度
#
#  输出文件:
#    highfreq_sensor_generated.log — 高频时序日志(JSON,每条1行)
#
#  输出字段(每条记录):
#    machineId, timestamp,
#    temperature(温度), vibration_x/vibration_y/vibration_z(三轴振动),
#    spindle_load, feed_rate, spindle_speed, is_running
#
#  数据特性:
#    - 高频采样: 默认每 1 秒一条 (可配置)
#    - 9 台设备轮询
#    - 运行状态: 温度/振动高、负载正常范围
#    - 待机状态:  温度/振动低、接近零
#    - 离线状态:  所有值归零或极低
#    - 偶发异常尖峰: 模拟故障前兆
#
#  用法:
#    bash generate_highfreq_sensor.sh          # 默认每1秒一条
#    bash generate_highfreq_sensor.sh 0.5      # 每0.5秒一条
#    nohup bash generate_highfreq_sensor.sh &  # 后台运行
#    Ctrl+C 停止
# ============================================================

# --- 配置 ---
INTERVAL_SEC=${1:-1}
OUT_FILE="highfreq_sensor_generated.log"

# 9 台设备
MACHINES=(109 110 111 112 113 114 115 116 117)

# ========== 各设备传感器基准值（不同设备略有差异） ==========
# 温度基准 (°C) — 运行时正常范围 35~65
declare -A BASE_TEMP
BASE_TEMP[109]=45; BASE_TEMP[110]=42; BASE_TEMP[111]=48
BASE_TEMP[112]=40; BASE_TEMP[113]=38; BASE_TEMP[114]=41
BASE_TEMP[115]=50; BASE_TEMP[116]=47; BASE_TEMP[117]=44

# 振动基准 (mm/s) — 运行时正常范围 0.5~4.0
declare -A BASE_VIB
BASE_VIB[109]=2.0; BASE_VIB[110]=1.8; BASE_VIB[111]=2.2
BASE_VIB[112]=1.5; BASE_VIB[113]=1.2; BASE_VIB[114]=1.4
BASE_VIB[115]=2.8; BASE_VIB[116]=2.5; BASE_VIB[117]=1.9

# 主轴负载基准 — 与 generate_change_data.sh 一致
declare -A BASE_LOAD
BASE_LOAD[109]=22; BASE_LOAD[110]=18; BASE_LOAD[111]=24
BASE_LOAD[112]=16; BASE_LOAD[113]=10; BASE_LOAD[114]=12
BASE_LOAD[115]=26; BASE_LOAD[116]=20; BASE_LOAD[117]=19

# 进给速度基准
declare -A BASE_FEED
BASE_FEED[109]=12000; BASE_FEED[110]=8000;  BASE_FEED[111]=15000
BASE_FEED[112]=6000;  BASE_FEED[113]=4000;  BASE_FEED[114]=5000
BASE_FEED[115]=20000; BASE_FEED[116]=10000; BASE_FEED[170]=9000

# 转速基准
declare -A BASE_SPEED
BASE_SPEED[109]=2200; BASE_SPEED[110]=1800; BASE_SPEED[111]=2400
BASE_SPEED[112]=1500; BASE_SPEED[113]=1200; BASE_SPEED[114]=1400
BASE_SPEED[115]=2800; BASE_SPEED[116]=2000; BASE_SPEED[117]=1900

# ========== 每台设备的内部状态 ==========
declare -A DEVICE_STATE      # 0=离线 1=待机 2=运行
declare -A TEMP_DRIFT        # 温度漂移量（缓慢变化）
declare -A VIB_DRIFT         # 振动漂移量
declare -A ANOMALY_COUNTER   # 异常计数器（控制偶发尖峰）

for m in "${MACHINES[@]}"; do
    DEVICE_STATE[$m]=2          # 初始默认运行
    TEMP_DRIFT[$m]=0
    VIB_DRIFT[$m]=0
    ANOMALY_COUNTER[$m]=0
done

echo "============================================"
echo "  CNC 高频传感器时序数据生成器"
echo "============================================"
echo "  输出间隔: ${INTERVAL_SEC}秒/条"
echo "  输出文件: $OUT_FILE (追加模式)"
echo "  设备数:  ${#MACHINES[@]} 台"
echo "  字段: machineId, timestamp, temperature,"
echo "        vibration_x/y/z, spindle_load,"
echo "        feed_rate, spindle_speed, is_running"
echo "--------------------------------------------"
echo "  启动时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "  按 Ctrl+C 可随时停止"
echo "============================================"

count=0

# 随机浮点数 (1位小数)
rand_float() {
    awk -v min="$1" -v max="$2" 'BEGIN{srand(); printf "%.1f", min+rand()*(max-min)}'
}

# 随机浮点数 (3位小数，用于振动)
rand_float3() {
    awk -v min="$1" -v max="$2" 'BEGIN{srand(); printf "%.3f", min+rand()*(max-min)}'
}

# 随机整数
rand_int() {
    awk -v min="$1" -v max="$2" 'BEGIN{printf "%d", int(min+rand()*(max-min+1))}'
}

# 生成正态分布随机数 (Box-Muller 近似，用于更真实的温度/振动波动)
# 用法: rand_normal $均值 $标准差
rand_normal() {
    local mean=$1 std=$2
    awk -v m="$mean" -v s="$std" 'BEGIN{
        u1=rand(); u2=rand();
        # Box-Muller 变换
        z=sqrt(-2*log(u1))*cos(2*3.14159265358979*u2);
        printf "%.1f", m + s*z
    }'
}

# ======== 主循环 ========
while true; do

    # 轮询所有设备（高频意味着每轮都遍历全部设备）
    for mid in "${MACHINES[@]}"; do

        ts=$(date '+%Y-%m-%d %H:%M:%S')

        # ---- 状态决定：模拟设备状态切换（比低频慢很多）----
        # 约 30 条数据(~30秒)才可能切换一次状态，保证窗口内数据相对稳定
        if [ $((RANDOM % 30)) -eq 0 ]; then
            r=$(( RANDOM % 100 ))
            if [ $r -lt 55 ]; then
                DEVICE_STATE[$mid]=2       # 运行 55%
            elif [ $r -lt 95 ]; then
                DEVICE_STATE[$mid]=1       # 待机 40%
            else
                DEVICE_STATE[$mid]=0       # 离线 5%
            fi
        fi

        state=${DEVICE_STATE[$mid]}

        # ---- 缓慢漂移（模拟真实传感器的温升/磨损趋势）----
        # 每 100 条左右漂移一点
        if [ $((RANDOM % 100)) -lt 3 ]; then
            drift=$(awk 'BEGIN{printf "%.1f", rand()*2-0.5}')  # -0.5 ~ +1.5
            TEMP_DRIFT[$mid]=$(awk -v v="${TEMP_DRIFT[$mid]}" -v d="$drift" '{
                nv=v+d; if(nv<-5)nv=-5; if(nv>15)nv=15; printf "%.1f", nv
            }')
            vib_drift=$(awk 'BEGIN{printf "%.3f", rand()*0.4-0.1}')
            VIB_DRIFT[$mid]=$(awk -v v="${VIB_DRIFT[$mid]}" -v d="$vib_drift" '{
                nv=v+d; if(nv<-0.5)nv=-0.5; if(nv>1.5)nv=1.5; printf "%.3f", nv
            }')
        fi

        # ---- 根据状态生成传感器数值 ----

        if [ "$state" -eq 2 ]; then
            # ====== 运行状态 ======
            is_running=1

            # 温度: 基准 + 漂移 + 正态噪声 (范围 30~75°C)
            base_t=${BASE_TEMP[$mid]:-45}
            temp_drift=${TEMP_DRIFT[$mid]}
            temperature=$(rand_normal $(awk -v b="$base_t" -v d="$temp_drift" 'BEGIN{printf "%.1f", b+d}') 4.0)
            # 边界钳制
            temperature=$(awk -v t="$temperature" 'BEGIN{if(t<25)t=25;if(t>80)t=80;printf "%.1f",t}')

            # 三轴振动: 基准 + 漂移 + 正态噪声 (范围 0.1~6.0 mm/s)
            base_v=${BASE_VIB[$mid]:-2.0}
            vib_d=${VIB_DRIFT[$mid]}
            vib_base=$(awk -v b="$base_v" -v d="$vib_d" 'BEGIN{printf "%.3f", b+d}')
            vibration_x=$(rand_normal "$vib_base" 0.5)
            vibration_y=$(rand_normal "$(awk -v v="$vib_base" 'BEGIN{printf "%.3f", v*0.8}')" 0.3)
            vibration_z=$(rand_normal "$(awk -v v="$vib_base" 'BEGIN{printf "%.3f", v*1.2}')" 0.6)

            # 振动边界钳制
            vibration_x=$(awk -v v="$vibration_x" 'BEGIN{if(v<0.05)v=0.05;if(v>8.0)v=8.0;printf "%.3f",v}')
            vibration_y=$(awk -v v="$vibration_y" 'BEGIN{if(v<0.03)v=0.03;if(v>6.0)v=6.0;printf "%.3f",v}')
            vibration_z=$(awk -v v="$vibration_z" 'BEGIN{if(v<0.08)v=0.08;if(v>10.0)v=10.0;printf "%.3f",v}')

            # 主轴负载 / 进给 / 转速 — 运行区间
            base_l=${BASE_LOAD[$mid]:-20}
            spindle_load=$(rand_float $((base_l-5)) $((base_l+10)))
            spindle_load=$(awk -v l="$spindle_load" 'BEGIN{if(l<8)l=8;if(l>32)l=32;printf "%.1f",l}')

            base_f=${BASE_FEED[$mid]:-10000}
            feed_rate=$(rand_int $((base_f-3000)) $((base_f+5000)))
            feed_rate=$(awk -v f="$feed_rate" 'BEGIN{if(f<200)f=200;if(f>33535)f=33535;printf "%d",f}')

            base_s=${BASE_SPEED[$mid]:-2000}
            spindle_speed=$(rand_int $((base_s-400)) $((base_s+600)))
            spindle_speed=$(awk -v s="$spindle_speed" 'BEGIN{if(s<500)s=500;if(s>3200)s=3200;printf "%d",s}')

        elif [ "$state" -eq 1 ]; then
            # ====== 待机状态 ======
            is_running=0

            # 温度: 缓慢降低到环境温度附近 (25~38°C)
            base_t=${BASE_TEMP[$mid]:-45}
            temperature=$(rand_float 25.0 38.0)

            # 振动: 极低 (0.01~0.5 mm/s)
            vibration_x=$(rand_float3 0.01 0.3)
            vibration_y=$(rand_float3 0.01 0.2)
            vibration_z=$(rand_float3 0.02 0.5)

            # 负载/进给/转速: 低区间
            spindle_load=$(rand_float 1.0 8.0)
            feed_rate=$(rand_int 35 800)
            spindle_speed=$(rand_int 144 600)

        else
            # ====== 离线状态 ======
            is_running=0

            # 全部接近零
            temperature=$(rand_float 18.0 28.0)
            vibration_x=$(rand_float3 0.000 0.05)
            vibration_y=$(rand_float3 0.000 0.04)
            vibration_z=$(rand_float3 0.000 0.08)
            spindle_load=$(rand_float 0.0 2.0)
            feed_rate=0
            spindle_speed=0
        fi

        # ---- 偶发异常尖峰 (模拟故障前兆, ~3%概率) ----
        # 用于让 Flink 的方差/峭度/FFT 能检测到异常
        if [ "$state" -eq 2 ] && [ $((RANDOM % 100)) -lt 3 ]; then
            anomaly_type=$(( RANDOM % 4 ))

            case $anomaly_type in
                0)  # 温度尖峰
                    temperature=$(awk -v t="$temperature" 'BEGIN{printf "%.1f", t+rand()*15+5}')
                    ;;
                1)  # 振动尖峰(X轴为主)
                    vibration_x=$(awk -v v="$vibration_x" 'BEGIN{printf "%.3f", v+rand()*3+1}')
                    vibration_z=$(awk -v v="$vibration_z" 'BEGIN{printf "%.3f", v+rand()*1.5+0.5}')
                    ;;
                2)  # 负载尖峰
                    spindle_load=$(awk -v l="$spindle_load" 'BEGIN{v=l+rand()*8+3; if(v>32)v=32; printf "%.1f",v}')
                    ;;
                3)  # 综合轻微异常（多参数同时偏离）
                    temperature=$(awk -v t="$temperature" 'BEGIN{printf "%.1f", t+rand()*8+2}')
                    vibration_x=$(awk -v v="$vibration_x" 'BEGIN{printf "%.3f", v+rand()*1+0.3}')
                    spindle_load=$(awk -v l="$spindle_load" 'BEGIN{v=l+rand()*4+1; if(v>32)v=32; printf "%.1f",v}')
                    ;;
            esac
        fi

        # ---- 温度二次边界检查（防止尖峰后超限）----
        temperature=$(awk -v t="$temperature" 'BEGIN{if(t<15)t=15;if(t>95)t=95;printf "%.1f",t}')
        vibration_x=$(awk -v v="$vibration_x" 'BEGIN{if(v<0)v=0;if(v>12)v=12;printf "%.3f",v}')
        vibration_y=$(awk -v v="$vibration_y" 'BEGIN{if(v<0)v=0;if(v>10)v=10;printf "%.3f",v}')
        vibration_z=$(awk -v v="$vibration_z" 'BEGIN{if(v<0)v=0;if(v>15)v=15;printf "%.3f",v}')

        # ========== 输出 JSON (10个字段) ==========
        json_line=$(printf '{"machineId":%s,"timestamp":"%s","temperature":%.1f,"vibration_x":%.3f,"vibration_y":%.3f,"vibration_z":%.3f,"spindle_load":%.1f,"feed_rate":%d,"spindle_speed":%d,"is_running":%d}' \
            "$mid" "$ts" "$temperature" "$vibration_x" "$vibration_y" "$vibration_z" \
            "$spindle_load" "$feed_rate" "$spindle_speed" "$is_running")

        echo "$json_line" >> "$OUT_FILE"
        count=$((count + 1))

    done  # end for each machine

    # 每轮(9条)打印一次进度
    total_this_round=$(( ${#MACHINES[@]} ))
    if $(( (count % total_this_round) == 0 )) 2>/dev/null; then
        :
    fi
    # 用更简单的方式：每90条(约10轮)打一次
    if [ $((count % 90)) -eq 0 ]; then
        echo "  [$ts] 已输出 ${count} 条高频记录 | 文件: $OUT_FILE"
    fi

    sleep "$INTERVAL_SEC"

done
