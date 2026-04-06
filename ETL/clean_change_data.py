# -*- coding: utf-8 -*-
"""
ChangeRecord 数据清洗脚本 - 14维特征版（中文段名）
从 log.txt 读取原始数据，解析 XML，提取 14 维特征，输出为 JSON
"""

import re
import json
import os
from datetime import datetime
from collections import defaultdict

# ======================== 配置 ========================
INPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log.txt")
OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cleaned_data.log")


def parse_xml_field(xml_string: str, col_name: str) -> str:
    """从 XML 字符串中提取指定 ColName 的值"""
    pattern = rf'<col ColName="{re.escape(col_name)}">(.*?)</col>'
    match = re.search(pattern, xml_string)
    if match:
        return match.group(1).strip()
    return None


def safe_float(value, default=0.0):
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    if value is None:
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def parse_alarm_no(alarm_str):
    if not alarm_str or alarm_str == "null":
        return 0
    try:
        info = json.loads(alarm_str)
        return safe_int(info.get("AlmNo", 0), 0)
    except (json.JSONDecodeError, TypeError):
        return 0


def parse_line(line: str, line_num: int):
    """解析一行 -> 14维特征字典"""
    line = line.strip()
    if not line:
        return None

    parts = line.split("\t")
    if len(parts) < 3:
        return None

    machine_id = parts[0].strip()
    record_state = parts[1].strip()
    xml_data = parts[2].strip()

    # 原始字段提取
    machine_status = parse_xml_field(xml_data, "机器状态") or record_state
    connection_status = parse_xml_field(xml_data, "连接状态") or "unknown"
    cutting_time = safe_float(parse_xml_field(xml_data, "切削时间"))
    cycle_time = safe_float(parse_xml_field(xml_data, "循环时间"))
    spindle_load = safe_float(parse_xml_field(xml_data, "主轴负载"))
    total_parts = safe_float(parse_xml_field(xml_data, "总加工个数"))
    run_time = safe_float(parse_xml_field(xml_data, "运行时间"))
    power_on_time = safe_float(parse_xml_field(xml_data, "上电时间"))
    spindle_speed = safe_float(parse_xml_field(xml_data, "主轴转速"))
    feed_speed = safe_float(parse_xml_field(xml_data, "进给速度"))
    alarm_raw = parse_xml_field(xml_data, "报警信息")
    alarm_no = parse_alarm_no(alarm_raw)

    # ====== 14 维特征（中文）======
    is_running = 1 if machine_status == "运行" else 0
    is_standby = 1 if machine_status == "待机" else 0
    is_offline = 1 if connection_status != "normal" else 0
    is_alarm = 1 if alarm_no != 0 else 0

    avg_spindle_load = round(spindle_load / run_time * 100, 2) if run_time and run_time > 0 and spindle_load else 0.0

    return {
        "设备编号": int(machine_id) if machine_id.isdigit() else machine_id,
        "是否运行": is_running,
        "是否待机": is_standby,
        "是否离线": is_offline,
        "是否报警": is_alarm,
        "持续时长(秒)": run_time,
        "切削时间(秒)": cutting_time,
        "循环时间(秒)": cycle_time,
        "主轴负载": spindle_load,
        "总加工个数": int(total_parts),
        "累计运行时间(秒)": power_on_time,
        "累计加工个数": int(total_parts),
        "平均主轴负载": avg_spindle_load,
        "主轴转速": spindle_speed,
        "进给速度": feed_speed,
    }


def main():
    print("=" * 50)
    print("  ChangeRecord 14维数据清洗")
    print("=" * 50)
    print(f"  输入: {INPUT_FILE}")
    print(f"  输出: {OUTPUT_FILE}")
    print("=" * 50)

    if not os.path.exists(INPUT_FILE):
        print(f"\n[错误] 文件不存在: {INPUT_FILE}")
        return

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()

    total = len(lines)
    results = []
    success = 0
    skip = 0
    machines = set()
    status_cnt = defaultdict(int)

    for i, line in enumerate(lines, 1):
        record = parse_line(line, i)
        if record is None:
            skip += 1
            continue
        results.append(record)
        success += 1
        machines.add(str(record["设备编号"]))
        status_cnt[record["是否运行"] and "运行" or (record["是否待机"] and "待机" or "离线")] += 1

    output = {
        "统计": {
            "总行数": total,
            "成功": success,
            "跳过": skip,
            "设备数": len(machines),
            "设备列表": sorted(machines),
            "状态分布": dict(status_cnt),
            "生成时间": datetime.now().isoformat(),
        },
        "数据": results,
    }

    # 输出：每条 JSON 一行 (NDJSON 格式)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        # 第一行写统计信息
        stat_line = json.dumps({"统计": output["统计"]}, ensure_ascii=False, separators=(",", ":"))
        f.write(stat_line + "\n")
        # 后面每条数据一行
        for record in results:
            line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
            f.write(line + "\n")

    size_kb = os.path.getsize(OUTPUT_FILE) / 1024
    print(f"\n  [OK] 清洗完成!")
    print(f"     总计: {total} 行 | 成功: {success} | 跳过: {skip}")
    print(f"     设备: {len(machines)} 台 ({', '.join(sorted(machines))})")
    for s, c in sorted(status_cnt.items(), key=lambda x: -x[1]):
        pct = c / success * 100 if success else 0
        print(f"     状态 [{s}]: {c} ({pct:.1f}%)")
    print(f"\n  [>>] 输出: {OUTPUT_FILE} ({size_kb:.1f} KB)")

    # 预览前2条
    print("\n  [*] 数据预览:")
    for idx, r in enumerate(results[:2], 1):
        print(f"\n  --- 记录#{idx} 设备{r['设备编号']} ---")
        for k, v in r.items():
            print(f"    {k}: {v}")


if __name__ == "__main__":
    main()
