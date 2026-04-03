package com.shtd.model

import java.sql.Timestamp

/**
 * 对应 `EnvironmentData` 表
 */
case class EnvironmentData(
                            EnvoId: String,        // 主键/记录ID
                            BaseID: String,        // 设备ID (用于分组计算)
                            CO2: String,           // 二氧化碳 (字符串)
                            PM25: String,          // PM2.5 (字符串)
                            PM10: String,          // PM10 (字符串)
                            Temperature: String,   // 温度 (字符串 - 核心特征)
                            Humidity: String,      // 湿度 (字符串)
                            TVOC: String,          // 总挥发性有机物
                            CH2O: String,          // 甲醛
                            Smoke: String,         // 烟雾
                            InPutTime: String      // 原始时间字符串
                          )

/**
 * 清洗后的数据类 (用于流处理/批处理计算)
 */
case class CleanEnvironmentData(
                                 deviceId: String,             // BaseID
                                 timestamp: Timestamp,         // 解析后的 InPutTime
                                 temperature: Double,          // 解析后的 Temperature
                                 humidity: Double,             // 解析后的 Humidity
                                 smoke: Double                 // 解析后的 Smoke (用于异常检测)
                               )

/**
 * 特征计算结果类 (最终输出)
 * 包含时域特征 (均值、方差、峭度) 和频域特征 (FFT)
 */
case class FeatureResult(
                          deviceId: String,             // 设备ID
                          windowStart: String,          // 窗口开始时间
                          windowEnd: String,            // 窗口结束时间
                          meanTemp: Double,             // 均值
                          stdDevTemp: Double,           // 标准差
                          kurtosisTemp: Double,         // 峭度 (Kurtosis)
                          fftMainFrequency: Double,     // FFT 主频
                          fftEnergy: Double,            // FFT 总能量
                          anomalyScore: Double          // 异常评分 (基于 Flink CEP 规则)
                        )