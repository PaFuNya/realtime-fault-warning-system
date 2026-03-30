package org.example.tasks

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import java.sql.Timestamp

// 数据类型定义
case class SensorData(machine_id: String, ts: Long, temperature: Double, vibration_x: Double, vibration_y: Double, vibration_z: Double, current: Double, noise: Double, speed: Double, timestamp: Timestamp)
case class DeviceState(var lastTs: Long, var lastTemp: Double, var lastVibX: Double, var status: String, var healthScore: Int)
case class EnrichedSensor(machine_id: String, timestamp: Timestamp, temperature: Double, vibration_x: Double, current: Double, speed: Double, temp_slope: Double, vib_increase_ratio: Double, status: String, health_score: Int)

/**
 * 模块二：实时数据处理引擎 
 * 2.1 填补短时缺失值（线性插值）
 * 2.2 实时特征计算 (状态机识别、温度上升斜率、振动突增率)
 */
object Task2_2_FeatureEngineering {

  def updateDeviceState(machineId: String, inputs: Iterator[SensorData], state: GroupState[DeviceState]): Iterator[EnrichedSensor] = {
    var currentState = if (state.exists) state.get else DeviceState(0L, 0.0, 0.0, "启动中", 100)
    val results = new scala.collection.mutable.ListBuffer[EnrichedSensor]()

    inputs.foreach { input =>
      // 2.1 流式清洗: 填补短时缺失值 (前向插值替代)
      val temp = if (input.temperature.isNaN) currentState.lastTemp else input.temperature
      val vibX = if (input.vibration_x.isNaN) currentState.lastVibX else input.vibration_x

      // 2.2 实时特征计算: 温度上升斜率, 振动突增率
      val timeDiff = if (currentState.lastTs > 0) (input.ts - currentState.lastTs) / 1000.0 else 0.0
      val tempSlope = if (timeDiff > 0) (temp - currentState.lastTemp) / timeDiff else 0.0
      val vibIncrease = if (currentState.lastVibX > 0) (vibX - currentState.lastVibX) / currentState.lastVibX else 0.0

      // 2.2 状态机识别
      if (input.speed > 1000 && currentState.status == "启动中") currentState.status = "稳定运行"
      else if (input.speed < 100) currentState.status = "异常停机"

      // 健康分扣减模拟
      if (vibIncrease > 0.2) currentState.healthScore = Math.max(0, currentState.healthScore - 5)

      currentState.lastTs = input.ts
      currentState.lastTemp = temp
      currentState.lastVibX = vibX

      results += EnrichedSensor(machineId, input.timestamp, temp, vibX, input.current, input.speed, tempSlope, vibIncrease, currentState.status, currentState.healthScore)
    }
    state.update(currentState)
    results.iterator
  }

  def getEnrichedStream(spark: SparkSession, sensorRawDF: DataFrame): DataFrame = {
    import spark.implicits._
    val sensorDS = sensorRawDF.as[SensorData]
    sensorDS
      .groupByKey(_.machine_id)
      .flatMapGroupsWithState(org.apache.spark.sql.streaming.OutputMode.Append(), GroupStateTimeout.NoTimeout())(updateDeviceState)
      .toDF()
  }
}