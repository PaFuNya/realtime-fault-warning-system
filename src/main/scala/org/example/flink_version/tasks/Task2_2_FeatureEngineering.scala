package org.example.flink_version.tasks

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

case class EnrichedSensorData(machine_id: String, ts: Long, temperature: Double, vibration_x: Double, current: Double, speed: Double, temp_slope: Double, vib_increase_ratio: Double, status: String, health_score: Int)

/**
 * 模块二：实时数据处理引擎
 * 2.1 填补短时缺失值（前向插值）
 * 2.2 实时特征计算 (状态机识别、温度上升斜率、振动突增率)
 */
object Task2_2_FeatureEngineering {

  class FeatureProcessFunction extends KeyedProcessFunction[String, SensorData, EnrichedSensorData] {
    lazy val lastTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("last-ts", classOf[Long]))
    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
    lazy val lastVibXState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-vib-x", classOf[Double]))
    lazy val statusState: ValueState[String] = getRuntimeContext.getState(new ValueStateDescriptor[String]("status", classOf[String]))
    lazy val healthState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("health", classOf[Int]))

    override def processElement(value: SensorData, ctx: KeyedProcessFunction[String, SensorData, EnrichedSensorData]#Context, out: Collector[EnrichedSensorData]): Unit = {
      // 1. 初始化或获取状态
      val lastTs = if (lastTsState.value() == 0L) value.ts else lastTsState.value()
      val lastTemp = if (lastTempState.value() == 0.0) value.temperature else lastTempState.value()
      val lastVibX = if (lastVibXState.value() == 0.0) value.vibration_x else lastVibXState.value()
      var status = if (statusState.value() == null) "启动中" else statusState.value()
      var health = if (healthState.value() == 0) 100 else healthState.value()

      // 2.1 填补缺失值 (前向插值)
      val temp = if (value.temperature.isNaN) lastTemp else value.temperature
      val vibX = if (value.vibration_x.isNaN) lastVibX else value.vibration_x

      // 2.2 实时特征计算
      val timeDiff = if (lastTs > 0) (value.ts - lastTs) / 1000.0 else 0.0
      val tempSlope = if (timeDiff > 0) (temp - lastTemp) / timeDiff else 0.0
      val vibIncrease = if (lastVibX > 0) (vibX - lastVibX) / lastVibX else 0.0

      // 2.2 状态机识别
      if (value.speed > 1000 && status == "启动中") status = "稳定运行"
      else if (value.speed < 100) status = "异常停机"

      // 健康分模拟扣减
      if (vibIncrease > 0.2) health = Math.max(0, health - 5)

      // 更新状态
      lastTsState.update(value.ts)
      lastTempState.update(temp)
      lastVibXState.update(vibX)
      statusState.update(status)
      healthState.update(health)

      out.collect(EnrichedSensorData(value.machine_id, value.ts, temp, vibX, value.current, value.speed, tempSlope, vibIncrease, status, health))
    }
  }

  def getEnrichedStream(sensorStream: DataStream[SensorData]): DataStream[EnrichedSensorData] = {
    sensorStream
      .keyBy(_.machine_id)
      .process(new FeatureProcessFunction)
  }
}