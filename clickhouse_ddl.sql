-- ==============================================================================
-- 数据库初始化
-- ==============================================================================
CREATE DATABASE IF NOT EXISTS shtd_ads;
USE shtd_ads;

-- ==============================================================================
-- 1. 实时报警表 (对应 Task 16: 实时异常检测与报警)
-- 接收温度报警和设备硬件报警
-- ==============================================================================
CREATE TABLE IF NOT EXISTS realtime_alerts (
    machine_id String COMMENT '设备唯一标识',
    alert_time DateTime COMMENT '报警发生时间',
    alert_type String COMMENT '报警类型: 比如 温度报警、硬件报警',
    alert_level String COMMENT '报警级别: 比如 WARNING, CRITICAL',
    alert_msg String COMMENT '报警详细信息',
    temperature Float64 COMMENT '发生报警时的温度',
    error_code String COMMENT '错误代码(硬件报警专属)'
) ENGINE = MergeTree()
ORDER BY (alert_time, machine_id)
PARTITION BY toYYYYMMDD(alert_time);

-- ==============================================================================
-- 2. 实时状态大屏展示表 (对应 Task 2.4: 实时大屏展示 / Redis 缓存)
-- 记录设备的实时特征，供前端大屏查询展示
-- ==============================================================================
CREATE TABLE IF NOT EXISTS device_realtime_status (
    machine_id String COMMENT '设备唯一标识',
    ts DateTime COMMENT '数据时间戳',
    temperature Float64 COMMENT '当前温度',
    vibration_x Float64 COMMENT 'X轴振动',
    vibration_y Float64 COMMENT 'Y轴振动',
    vibration_z Float64 COMMENT 'Z轴振动',
    current Float64 COMMENT '当前电流',
    noise Float64 COMMENT '噪声分贝',
    speed Float64 COMMENT '转速',
    status String COMMENT '当前状态 (启动中/稳定运行/异常停机)',
    health_score Float64 COMMENT '实时健康评分'
) ENGINE = ReplacingMergeTree()
ORDER BY (machine_id)
PARTITION BY toYYYYMMDD(ts);

-- ==============================================================================
-- 3. 滑动窗口统计表 (对应 Task 15: 1分钟/5分钟/1小时窗口统计)
-- 记录设备的周期性聚合指标
-- ==============================================================================
CREATE TABLE IF NOT EXISTS realtime_status_window (
    window_start DateTime COMMENT '窗口开始时间',
    window_end DateTime COMMENT '窗口结束时间',
    machine_id String COMMENT '设备唯一标识',
    avg_temperature Float64 COMMENT '窗口内平均温度',
    max_vibration Float64 COMMENT '窗口内最大振动值',
    min_speed Float64 COMMENT '窗口内最低转速'
) ENGINE = MergeTree()
ORDER BY (window_end, machine_id)
PARTITION BY toYYYYMMDD(window_end);

-- ==============================================================================
-- 4. 连续参数偏差统计表 (对应 Task 18: 参数偏差计算与状态评估)
-- 记录30秒内的特征变化及最终扣分结果
-- ==============================================================================
CREATE TABLE IF NOT EXISTS process_deviation_stats (
    machine_id String COMMENT '设备唯一标识',
    window_start DateTime COMMENT '30秒窗口开始时间',
    window_end DateTime COMMENT '30秒窗口结束时间',
    temp_diff Float64 COMMENT '窗口内温度最大偏差',
    vib_diff Float64 COMMENT '窗口内振动最大偏差',
    current_diff Float64 COMMENT '窗口内电流最大偏差',
    score_deduction Float64 COMMENT '本窗口扣减的健康分',
    final_health_score Float64 COMMENT '扣分后的最终健康分'
) ENGINE = MergeTree()
ORDER BY (window_end, machine_id)
PARTITION BY toYYYYMMDD(window_end);
