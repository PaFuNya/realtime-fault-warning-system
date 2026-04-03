package edu.feature.bean

case class SensorRaw(
  EnvoId: String,
  BaseID: String,
  CO2: String, // 注意：数据库里是 Varchar，需要转 Double
  PM25: String,
  PM10: String,
  Temperature: String,
  Humidity: String,
  TVOC: String,
  CH2O: String,
  Smoke: String,
  InPutTime: String // 需要转 Timestamp
)
