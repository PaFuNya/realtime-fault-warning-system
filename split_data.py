# -*- coding: utf-8 -*-
"""
将 cleaned_data.log 拆分为两个文件：
  1. device_state.log   — 状态日志类（来自日志记录）
  2. sensor_metrics.log — 传感器/控制器读数类（来自设备实时快照）
"""

import json
import os
from datetime import datetime

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ETL")
INPUT_FILE = os.path.join(BASE_DIR, "cleaned_data.log")
STATE_OUTPUT = os.path.join(BASE_DIR, "device_state.log")
METRICS_OUTPUT = os.path.join(BASE_DIR, "sensor_metrics.log")

# ====== 状态日志类字段（来自日志记录）======
STATE_FIELDS = [
    "设备编号",
    "是否运行",       # isRunning  - 来自 ChangeRecordState
    "是否待机",       # isStandby  - 来自 ChangeRecordState
    "是否离线",       # isOffline  - 来自连接状态解析
    "是否报警",       # isAlarm    - 报警信息解析
    "持续时长(秒)",   # durationSeconds - 时间计算
]

# ====== 传感器/控制器读数类字段（来自设备实时快照）======
METRICS_FIELDS = [
    "设备编号",
    "切削时间(秒)",   # cuttingTime  - PLC采集物理量
    "循环时间(秒)",   # cycleTime    - PLC采集物理量
    "主轴负载",       # spindleLoad  - PLC传感器
    "总加工个数",     # totalParts   - PLC计数器
    "累计运行时间(秒)",# cumRuntime  - 上电时间累计
    "累计加工个数",   # cumParts
    "平均主轴负载",   # avgSpindleLoad - 计算值
    "主轴转速",       # spindleSpeed - 传感器
    "进给速度",       # feedSpeed    - 传感器
]


def main():
    print("=" * 50)
    print("  数据拆分: 状态日志 vs 传感器指标")
    print("=" * 50)

    if not os.path.exists(INPUT_FILE):
        print(f"[错误] 文件不存在: {INPUT_FILE}")
        return

    state_records = []
    metrics_records = []
    total = 0

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            record = json.loads(line)

            # 跳过统计行
            if "统计" in record:
                total = record["统计"].get("成功", 0)
                continue

            # 提取状态日志子集
            state_rec = {k: record[k] for k in STATE_FIELDS if k in record}
            state_records.append(state_rec)

            # 提取传感器指标子集
            metrics_rec = {k: record[k] for k in METRICS_FIELDS if k in record}
            metrics_records.append(metrics_rec)

    # 写入 device_state.log（NDJSON）
    stat_info = {
        "说明": "状态日志类数据 - 来自ChangeRecord日志解析",
        "字段": STATE_FIELDS,
        "记录数": len(state_records),
        "生成时间": datetime.now().isoformat(),
    }
    with open(STATE_OUTPUT, "w", encoding="utf-8") as f:
        f.write(json.dumps({"统计": stat_info}, ensure_ascii=False, separators=(",", ":")) + "\n")
        for rec in state_records:
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")

    # 写入 sensor_metrics.log（NDJSON）
    met_stat_info = {
        "说明": "传感器/控制器读数类数据 - 来自PLC设备实时快照",
        "字段": METRICS_FIELDS,
        "记录数": len(metrics_records),
        "生成时间": datetime.now().isoformat(),
    }
    with open(METRICS_OUTPUT, "w", encoding="utf-8") as f:
        f.write(json.dumps({"统计": met_stat_info}, ensure_ascii=False, separators=(",", ":")) + "\n")
        for rec in metrics_records:
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")

    state_size = os.path.getsize(STATE_OUTPUT) / 1024
    metrics_size = os.path.getsize(METRICS_OUTPUT) / 1024

    print(f"\n  [OK] 拆分完成!")
    print(f"     总记录数: {total}")
    print(f"\n  [>>] device_state.log   ({state_size:.1f} KB)")
    print(f"      字段({len(STATE_FIELDS)}个): {', '.join(STATE_FIELDS)}")
    print(f"\n  [>>] sensor_metrics.log ({metrics_size:.1f} KB)")
    print(f"      字段({len(METRICS_FIELDS)}个): {', '.join(METRICS_FIELDS)}")

    # 预览各前2条
    print(f"\n  [*] device_state.log 预览:")
    for i, r in enumerate(state_records[:2], 1):
        print(f"     ---#{i}---")
        for k, v in r.items():
            print(f"       {k}: {v}")

    print(f"\n  [*] sensor_metrics.log 预览:")
    for i, r in enumerate(metrics_records[:2], 1):
        print(f"     ---#{i}---")
        for k, v in r.items():
            print(f"       {k}: {v}")


if __name__ == "__main__":
    main()
