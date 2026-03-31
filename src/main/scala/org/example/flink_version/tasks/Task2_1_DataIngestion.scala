package org.example.flink_version.tasks

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import com.alibaba.fastjson.JSON
import java.util.Properties

// 数据模型定义
case class SensorData(machine_id: String, ts: Long, temperature: Double, vibration_x: Double, vibration_y: Double, vibration_z: Double, current: Double, noise: Double, speed: Double)
case class LogData(machine_id: String, ts: Long, error_code: String, error_msg: String, stack_trace: String)

/**
 * 模块二：实时数据处理引擎
 * 2.1 实时数据接入
 * Flink 消费 Kafka sensor_raw 和 log_raw 主题。
 * 解析 JSON，提取字段。
 */
object Task2_1_DataIngestion {
  val kafkaBrokers = "100.126.226.67:9092,100.90.72.128:9092,100.123.80.25:9092"

  def getSensorStream(env: StreamExecutionEnvironment): DataStream[SensorData] = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", kafkaBrokers)
    props.setProperty("group.id", "flink_sensor_group")
    props.setProperty("auto.offset.reset", "latest")

    val consumer = new FlinkKafkaConsumer[String]("sensor_raw", new SimpleStringSchema(), props)
    env.addSource(consumer).map { jsonStr =>
      val jsonObj = JSON.parseObject(jsonStr)
      SensorData(
        jsonObj.getString("machine_id"),
        jsonObj.getLong("ts"),
        jsonObj.getDouble("temperature"),
        jsonObj.getDouble("vibration_x"),
        jsonObj.getDouble("vibration_y"),
        jsonObj.getDouble("vibration_z"),
        jsonObj.getDouble("current"),
        jsonObj.getDouble("noise"),
        jsonObj.getDouble("speed")
      )
    }.assignAscendingTimestamps(_.ts)
  }

  def getLogStream(env: StreamExecutionEnvironment): DataStream[LogData] = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", kafkaBrokers)
    props.setProperty("group.id", "flink_log_group")
    props.setProperty("auto.offset.reset", "latest")

    val consumer = new FlinkKafkaConsumer[String]("log_raw", new SimpleStringSchema(), props)
    env.addSource(consumer).map { jsonStr =>
      val jsonObj = JSON.parseObject(jsonStr)
      LogData(
        jsonObj.getString("machine_id"),
        jsonObj.getLong("ts"),
        jsonObj.getString("error_code"),
        jsonObj.getString("error_msg"),
        jsonObj.getString("stack_trace")
      )
    }.assignAscendingTimestamps(_.ts)
  }
}