package org.example.tasks

import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerRecord,
  ProducerConfig
}
import java.util.Properties
import java.sql.DriverManager

object TestProducer {
  def main(args: Array[String]): Unit = {
    // 1. 获取 ClickHouse 中最大的时间戳作为基准，确保我们的测试数据比它新！
    var baseTimeMs = System.currentTimeMillis()
    try {
      Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
      val conn = DriverManager.getConnection(SparkUtils.ckUrl, "default", "")
      val stmt = conn.createStatement()
      val rs =
        stmt.executeQuery("SELECT max(update_time) FROM device_realtime_status")
      if (rs.next()) {
        val maxTs = rs.getTimestamp(1)
        if (maxTs != null) {
          baseTimeMs = maxTs.getTime + 5000 // 比表里最新的数据还要新 5 秒
        }
      }
      rs.close()
      stmt.close()
      conn.close()
    } catch {
      case e: Exception =>
        println(
          "Failed to get max time from CK, using current time: " + e.getMessage
        )
    }

    val props = new Properties()
    props.put(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      "master:9092,slave1:9092,slave2:9092"
    )
    props.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    val producer = new KafkaProducer[String, String](props)

    val now = baseTimeMs

    val data = Seq(
      // 场景 1: 正常
      (
        "sensor_raw",
        s"""{"machine_id":"101","ts":${now},"temperature":45.0,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":30.0,"speed":3000.0}"""
      ),
      // 场景 2: 高温
      (
        "sensor_raw",
        s"""{"machine_id":"102","ts":${now + 1000},"temperature":85.5,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":30.0,"speed":3000.0}"""
      ),
      // 场景 3: 日志错误
      (
        "log_raw",
        s"""{"machine_id":"103","ts":${now + 2000},"error_code":"999","error_message":"检测到严重硬件故障","stack_trace":"java.lang.OutOfMemoryError..."}"""
      ),
      // 场景 4: 异常停机 (为了演示清晰，我们只发一次)
      (
        "sensor_raw",
        s"""{"machine_id":"104","ts":${now + 3000},"temperature":45.0,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":30.0,"speed":500.0}"""
      ),
      // 场景 5: 工艺偏离 (为了演示清晰，我们只发一次)
      (
        "sensor_raw",
        s"""{"machine_id":"105","ts":${now + 5000},"temperature":45.0,"vibration_x":0.5,"vibration_y":0.5,"vibration_z":0.5,"current":35.0,"speed":3000.0}"""
      )
    )

    data.foreach { case (topic, json) =>
      val record = new ProducerRecord[String, String](topic, null, json)
      producer.send(record)
      println(s"Sent to $topic: $json")
      Thread.sleep(500)
    }

    producer.flush()
    producer.close()
    println("Done sending test data.")
  }
}
