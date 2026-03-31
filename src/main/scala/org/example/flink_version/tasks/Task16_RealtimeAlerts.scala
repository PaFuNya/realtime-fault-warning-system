package org.example.flink_version.tasks

import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import java.sql.PreparedStatement
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature

case class AlertResult(alert_time: Long, machine_id: String, alert_type: String, suggested_action: String)

/**
 * 任务组 5：实时指标与推理 (Flink -> ClickHouse)
 * 16. 实时异常检测与报警
 * 源：Kafka sensor_raw + log_raw 。
 * 规则引擎：温度 > 阈值 OR 振动突增 > 20% (sensor), OR Error Code 999 (log)
 */
object Task16_RealtimeAlerts {

  def startAlertSinks(enrichedStream: DataStream[EnrichedSensorData], logStream: DataStream[LogData], ckUrl: String, kafkaBrokers: String): Unit = {
    
    val sensorAlerts = enrichedStream
      .filter(d => d.temperature > 80.0 || d.vib_increase_ratio > 0.2)
      .map { d =>
        val aType = if (d.temperature > 80.0) "High Temperature" else "Vibration Sudden Increase > 20%"
        AlertResult(d.ts, d.machine_id, aType, "Inspect machine immediately")
      }

    val logAlerts = logStream
      .filter(_.error_code == "999")
      .map { d =>
        AlertResult(d.ts, d.machine_id, "Critical Log Error 999", "Check hardware logs")
      }

    val combinedAlerts = sensorAlerts.union(logAlerts)

    // 1. 写入 ClickHouse shtd_ads.realtime_alerts
    combinedAlerts.addSink(
      JdbcSink.sink(
        "INSERT INTO realtime_alerts (alert_time, machine_id, alert_type, suggested_action) VALUES (?, ?, ?, ?)",
        new JdbcStatementBuilder[AlertResult] {
          override def accept(ps: PreparedStatement, t: AlertResult): Unit = {
            ps.setLong(1, t.alert_time)
            ps.setString(2, t.machine_id)
            ps.setString(3, t.alert_type)
            ps.setString(4, t.suggested_action)
          }
        },
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withUrl(ckUrl)
          .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
          .withUsername("default")
          .withPassword("123456")
          .build()
      )
    )

    // 2. 推送至 Kafka alert_topic (模块 2.4 要求)
    val kafkaProducer = new FlinkKafkaProducer[String](
      kafkaBrokers,
      "alert_topic",
      new SimpleStringSchema()
    )
    
    combinedAlerts.map(a => JSON.toJSONString(a, new Array[SerializerFeature](0): _*)).addSink(kafkaProducer)
  }
}