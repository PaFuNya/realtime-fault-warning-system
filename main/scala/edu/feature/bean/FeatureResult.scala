package edu.feature.bean

case class FeatureResult(
                          deviceId: String,
                          timestamp: Long,
                          mean: Double,
                          variance: Double,
                          kurtosis: Double, // 峭度
                          fftPeak: Double,  // FFT频谱峰值
                          statusLabel: String // 标签 (正常/异常)
                        )
