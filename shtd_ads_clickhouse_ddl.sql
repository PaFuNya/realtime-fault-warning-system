-- ClickHouse 实时数据处理引擎 建表语句
-- 目标数据库：shtd_ads
CREATE DATABASE IF NOT EXISTS shtd_ads;

-- 15. 实时状态窗口统计表
-- 目标：ClickHouse shtd_ads.realtime_status_window
-- 逻辑：1 分钟滚动窗口，计算各设备温度最大值、振动 RMS 均值。
CREATE TABLE IF NOT EXISTS shtd_ads.realtime_status_window (
    window_start DateTime COMMENT '窗口开始时间',
    window_end DateTime COMMENT '窗口结束时间',
    machine_id String COMMENT '设备ID',
    max_temperature Float64 COMMENT '温度最大值',
    avg_vib_rms Float64 COMMENT '振动 RMS 均值',
    insert_time DateTime DEFAULT now() COMMENT '数据写入时间'
) ENGINE = MergeTree ()
PARTITION BY
    toYYYYMMDD (window_start)
ORDER BY (window_start, machine_id) TTL window_start + INTERVAL 30 DAY SETTINGS index_granularity = 8192;

-- 16. 实时异常检测与报警表
-- 目标：ClickHouse shtd_ads.realtime_alerts
-- 逻辑：规则引擎：温度 > 阈值 OR 振动突增 > 20%。输出：报警时间，设备 ID, 报警类型，建议措施。
CREATE TABLE IF NOT EXISTS ldc.realtime_alerts (
    alert_time DateTime COMMENT '报警时间',
    machine_id String COMMENT '设备ID',
    alert_type String COMMENT '报警类型 (例如: 温度过高, 振动突增)',
    trigger_value Float64 COMMENT '触发报警的实际值',
    threshold_value Float64 COMMENT '报警阈值',
    suggested_action String COMMENT '建议措施',
    insert_time DateTime DEFAULT now() COMMENT '数据写入时间'
) ENGINE = MergeTree ()
PARTITION BY
    toYYYYMMDD (alert_time)
ORDER BY (
        alert_time,
        machine_id,
        alert_type
    ) TTL alert_time + INTERVAL 90 DAY SETTINGS index_granularity = 8192;

-- 17. 实时 RUL 动态预测表
-- 目标：ClickHouse shtd_ads.realtime_rul_monitor
-- 逻辑：每分钟输出一次各设备的剩余寿命预测值及风险等级 (High/Medium/Low)。
CREATE TABLE IF NOT EXISTS ldc.realtime_rul_monitor (
    predict_time DateTime COMMENT '预测时间 (通常为窗口结束时间)',
    machine_id String COMMENT '设备ID',
    predicted_rul Float64 COMMENT '剩余寿命预测值 (小时/天，根据模型定)',
    risk_level String COMMENT '风险等级 (High/Medium/Low)',
    failure_probability Float64 COMMENT '故障概率 (0.0 ~ 1.0)',
    insert_time DateTime DEFAULT now() COMMENT '数据写入时间'
) ENGINE = MergeTree ()
PARTITION BY
    toYYYYMMDD (predict_time)
ORDER BY (
        predict_time,
        machine_id,
        risk_level
    ) TTL predict_time + INTERVAL 30 DAY SETTINGS index_granularity = 8192;

-- 18. 实时工艺参数偏离监控表
-- 目标：ClickHouse shtd_ads.realtime_process_deviation
-- 逻辑：对比实时电流/转速与标准值的偏差，持续偏离 30 秒即记录。
CREATE TABLE IF NOT EXISTS ldc.realtime_process_deviation (
    record_time DateTime COMMENT '偏离记录时间',
    machine_id String COMMENT '设备ID',
    param_name String COMMENT '偏离参数名称 (例如: current, speed)',
    actual_value Float64 COMMENT '实际值',
    standard_value Float64 COMMENT '标准值',
    deviation_ratio Float64 COMMENT '偏离比例 (%)',
    duration_seconds Int32 COMMENT '持续偏离时长 (秒)',
    insert_time DateTime DEFAULT now() COMMENT '数据写入时间'
) ENGINE = MergeTree ()
PARTITION BY
    toYYYYMMDD (record_time)
ORDER BY (
        record_time,
        machine_id,
        param_name
    ) TTL record_time + INTERVAL 30 DAY SETTINGS index_granularity = 8192;

-- 2.4 实时结果输出 - 实时指标表
-- 目标：ClickHouse shtd_ads.device_realtime_status
-- 逻辑：写入实时指标，供大屏展示。为了支持大屏展示最新状态，采用 ReplacingMergeTree，
--      按设备ID进行去重/更新，保证每次查询到的都是该设备的最新状态。
CREATE TABLE IF NOT EXISTS shtd_ads.device_realtime_status (
    machine_id String COMMENT '设备ID',
    update_time DateTime COMMENT '更新时间',
    machine_status String COMMENT '设备当前状态 (启动中/稳定运行/异常停机)',
    current_temperature Float64 COMMENT '当前温度',
    current_vibration_rms Float64 COMMENT '当前振动RMS',
    current_rul Float64 COMMENT '当前预测剩余寿命',
    health_score Float64 COMMENT '健康评分 (0-100)',
    insert_time DateTime DEFAULT now() COMMENT '数据写入时间'
) ENGINE = ReplacingMergeTree(update_time)
PARTITION BY toYYYYMMDD(update_time)
ORDER BY (machine_id)
TTL update_time + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;