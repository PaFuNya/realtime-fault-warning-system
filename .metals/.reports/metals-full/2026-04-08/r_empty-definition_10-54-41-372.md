error id: file:///D:/Desktop/Match/MATCH/OverMatch/src/main/scala/org/example/tasks/FlinkRealtimeEngine.scala:scala.
file:///D:/Desktop/Match/MATCH/OverMatch/src/main/scala/org/example/tasks/FlinkRealtimeEngine.scala
empty definition using pc, found symbol in pc: scala.
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -org/apache/flink/streaming/api/scala/org/apache/flink/streaming/api/scala.
	 -org/apache/flink/streaming/api/scala.
	 -scala/Predef.org.apache.flink.streaming.api.scala.
offset: 622
uri: file:///D:/Desktop/Match/MATCH/OverMatch/src/main/scala/org/example/tasks/FlinkRealtimeEngine.scala
text:
```scala
package org.example.tasks

import org.apache.flink.api.common.functions.{RichMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.sc@@ala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.util.Collector
import com.alibaba.fastjson.JSON
import redis.clients.jedis.Jedis
import java.sql.{PreparedStatement, Timestamp}
import java.util.Properties

object FlinkRealtimeEngine {

  case class SensorRaw(
      machine_id: String,
      ts: Long,
      temperature: Double,
      vibration_x: Double,
      vibration_y: Double,
      vibration_z: Double,
      current: Double,
      noise: Double,
      speed: Double,
      timestamp: Timestamp
  )

  case class LogRaw(
      machine_id: String,
      ts: Long,
      error_code: String,
      error_msg: String,
      stack_trace: String,
      timestamp: Timestamp
  )

  case class ChangeRecord(
      machine_id: String,
      status: String,
      start_time: Timestamp,
      end_time: Timestamp
  )

  case class WindowMetrics(
      window_start: Timestamp,
      window_end: Timestamp,
      machine_id: String,
      max_temperature: Double,
      avg_vib_rms: Double,
      current_fluctuation: Double,
      temp_slope: Double,
      window_type: String
  )

  case class AlertRecord(
      alert_time: Timestamp,
      machine_id: String,
      alert_type: String,
      trigger_value: Double,
      threshold_value: Double,
      suggested_action: String,
      insert_time: Timestamp
  )

  case class RealtimeStatus(
      machine_id: String,
      update_time: Timestamp,
      machine_status: String,
      current_temperature: Double,
      current_vibration_rms: Double,
      health_score: Double
  )

  case class ProcessDeviation(
      record_time: Timestamp,
      machine_id: String,
      param_name: String,
      actual_value: Double,
      standard_value: Double,
      deviation_ratio: Double,
      duration_seconds: Int
  )

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.enableCheckpointing(10000)

    val tableEnv = StreamTableEnvironment.create(env)

    val kafkaBrokers = AppConfig.getString("kafka.brokers", "master:9092,slave1:9092,slave2:9092")
    val ckUrl = AppConfig.getString("clickhouse.url", "jdbc:clickhouse://master:8123/ldc")
    val redisHost = AppConfig.getString("redis.host", "master")
    val redisPort = AppConfig.getInt("redis.port", 6379)
    val hdfsUrl = "hdfs://master:8020"

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", kafkaBrokers)
    kafkaProps.setProperty("group.id", "flink-realtime-engine")

    // ==========================================
    // 模块 2.1 实时数据接入
    // ==========================================
    val sensorConsumer = new FlinkKafkaConsumer[String]("sensor_raw", new SimpleStringSchema(), kafkaProps)
    sensorConsumer.setStartFromLatest()
    val sensorStream = env.addSource(sensorConsumer).map { json =>
      val obj = JSON.parseObject(json)
      SensorRaw(
        obj.getString("machine_id"),
        obj.getLongValue("ts"),
        obj.getDoubleValue("temperature"),
        obj.getDoubleValue("vibration_x"),
        obj.getDoubleValue("vibration_y"),
        obj.getDoubleValue("vibration_z"),
        obj.getDoubleValue("current"),
        obj.getDoubleValue("noise"),
        obj.getDoubleValue("speed"),
        new Timestamp(obj.getLongValue("ts"))
      )
    }

    val logConsumer = new FlinkKafkaConsumer[String]("log_raw", new SimpleStringSchema(), kafkaProps)
    logConsumer.setStartFromLatest()
    val logStream = env.addSource(logConsumer).map { json =>
      val obj = JSON.parseObject(json)
      LogRaw(
        obj.getString("machine_id"),
        obj.getLongValue("ts"),
        obj.getString("error_code"),
        obj.getString("error_msg"),
        obj.getString("stack_trace"),
        new Timestamp(obj.getLongValue("ts"))
      )
    }

    val changeConsumer = new FlinkKafkaConsumer[String]("ChangeRecord", new SimpleStringSchema(), kafkaProps)
    changeConsumer.setStartFromLatest()
    val changeStream = env.addSource(changeConsumer).flatMap { csv =>
      try {
        val parts = csv.split(",")
        if (parts.length >= 6) {
          Some(ChangeRecord(
            parts(1),
            parts(3),
            Timestamp.valueOf(parts(4)),
            Timestamp.valueOf(parts(5))
          ))
        } else None
      } catch { case _: Exception => None }
    }

    // ==========================================
    // 模块 2.2 状态机识别与流计算
    // ==========================================
    // 1. 更新 ChangeRecord 到 Redis
    changeStream.map(new RichMapFunction[ChangeRecord, Unit] {
      var jedis: Jedis = _
      override def open(parameters: Configuration): Unit = {
        jedis = new Jedis(redisHost, redisPort)
      }
      override def map(value: ChangeRecord): Unit = {
        if (value.machine_id != null && value.status != null) {
          jedis.hset("change_record_status", value.machine_id, value.status)
        }
      }
      override def close(): Unit = if (jedis != null) jedis.close()
    })

    // 2. 关联 Redis 计算状态和健康分
    val enrichedSensorStream = sensorStream.map(new RichMapFunction[SensorRaw, RealtimeStatus] {
      var jedis: Jedis = _
      override def open(parameters: Configuration): Unit = {
        jedis = new Jedis(redisHost, redisPort)
      }
      override def map(sensor: SensorRaw): RealtimeStatus = {
        val recordStatus = Option(jedis.hget("change_record_status", sensor.machine_id)).getOrElse("")
        
        val machineStatus = if (recordStatus == "预警") "异常停机"
        else if (sensor.speed < 1000) "启动中"
        else "稳定运行"

        val healthScore = machineStatus match {
          case "稳定运行" => 100.0
          case "启动中" => 80.0
          case _ => 60.0
        }

        // 写入 Redis device_health (限制频率每分钟1次)
        val nowSec = System.currentTimeMillis() / 1000
        val lastKey = "device_health_update_time:" + sensor.machine_id
        val lastStr = jedis.get(lastKey)
        val doUpdate = if (lastStr == null) true else (nowSec - lastStr.toLong) >= 60
        if (doUpdate) {
          jedis.hset("device_health", sensor.machine_id, healthScore.toString)
          jedis.set(lastKey, nowSec.toString)
        }

        RealtimeStatus(
          sensor.machine_id,
          sensor.timestamp,
          machineStatus,
          sensor.temperature,
          sensor.vibration_x, // 原始 Spark 代码中使用 vibration_x 作为 current_vibration_rms
          healthScore
        )
      }
      override def close(): Unit = if (jedis != null) jedis.close()
    })

    // 写入 ClickHouse device_realtime_status
    enrichedSensorStream.addSink(JdbcSink.sink(
      "INSERT INTO device_realtime_status (machine_id, update_time, machine_status, current_temperature, current_vibration_rms, health_score) VALUES (?, ?, ?, ?, ?, ?)",
      new JdbcStatementBuilder[RealtimeStatus] {
        override def accept(ps: PreparedStatement, t: RealtimeStatus): Unit = {
          ps.setString(1, t.machine_id)
          ps.setTimestamp(2, t.update_time)
          ps.setString(3, t.machine_status)
          ps.setDouble(4, t.current_temperature)
          ps.setDouble(5, t.current_vibration_rms)
          ps.setDouble(6, t.health_score)
        }
      },
      JdbcExecutionOptions.builder().withBatchSize(100).build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(ckUrl).withDriverName("ru.yandex.clickhouse.ClickHouseDriver").build()
    ))

    // ==========================================
    // 任务 15: 实时状态窗口统计 (1m, 5m, 1h)
    // ==========================================
    def computeWindowMetrics(windowType: String, size: Time) = {
      sensorStream
        .keyBy(_.machine_id)
        .window(TumblingProcessingTimeWindows.of(size))
        .process(new org.apache.flink.streaming.api.scala.function.ProcessWindowFunction[SensorRaw, WindowMetrics, String, TimeWindow] {
          override def process(key: String, context: Context, elements: Iterable[SensorRaw], out: Collector[WindowMetrics]): Unit = {
            var maxTemp = Double.MinValue
            var minTemp = Double.MaxValue
            var sumVibSq = 0.0
            var sumCurrent = 0.0
            var sumCurrentSq = 0.0
            var count = 0

            elements.foreach { s =>
              if (s.temperature > maxTemp) maxTemp = s.temperature
              if (s.temperature < minTemp) minTemp = s.temperature
              sumVibSq += (s.vibration_x * s.vibration_x + s.vibration_y * s.vibration_y + s.vibration_z * s.vibration_z)
              sumCurrent += s.current
              sumCurrentSq += s.current * s.current
              count += 1
            }

            if (count > 0) {
              val avgVibRms = math.sqrt(sumVibSq / count)
              val meanCurrent = sumCurrent / count
              val currentFluctuation = math.sqrt((sumCurrentSq / count) - (meanCurrent * meanCurrent))
              val tempSlope = maxTemp - minTemp

              out.collect(WindowMetrics(
                new Timestamp(context.window.getStart),
                new Timestamp(context.window.getEnd),
                key, maxTemp, avgVibRms, currentFluctuation, tempSlope, windowType
              ))
            }
          }
        })
    }

    val window1m = computeWindowMetrics("1m", Time.minutes(1))
    val window5m = computeWindowMetrics("5m", Time.minutes(5))
    val window1h = computeWindowMetrics("1h", Time.hours(1))

    // 1m 窗口后处理：插值填补 & 振动突增检测
    val interpolated1m = window1m
      .keyBy(_.machine_id)
      .process(new KeyedProcessFunction[String, WindowMetrics, WindowMetrics] {
        var lastMetricsState: ValueState[WindowMetrics] = _
        var prevVibRmsState: ValueState[Double] = _
        var jedis: Jedis = _

        override def open(parameters: Configuration): Unit = {
          lastMetricsState = getRuntimeContext.getState(new ValueStateDescriptor("lastMetrics", classOf[WindowMetrics]))
          prevVibRmsState = getRuntimeContext.getState(new ValueStateDescriptor("prevVibRms", classOf[Double]))
          jedis = new Jedis(redisHost, redisPort)
        }

        override def processElement(value: WindowMetrics, ctx: KeyedProcessFunction[String, WindowMetrics, WindowMetrics]#Context, out: Collector[WindowMetrics]): Unit = {
          val lastMetrics = lastMetricsState.value()
          
          // 插值逻辑 (gap > 1s && gap <= 5s) - 模拟 Spark 逻辑
          val finalMetrics = if (lastMetrics != null) {
            val gapSec = (value.window_end.getTime - lastMetrics.window_end.getTime) / 1000
            if (gapSec > 1 && gapSec <= 5) {
              value.copy(
                max_temperature = lastMetrics.max_temperature,
                avg_vib_rms = lastMetrics.avg_vib_rms,
                current_fluctuation = lastMetrics.current_fluctuation
              )
            } else value
          } else value

          // 振动突增报警
          val prevVib = prevVibRmsState.value()
          if (prevVib > 0.0) {
            val pct = math.abs(finalMetrics.avg_vib_rms - prevVib) / prevVib
            if (pct >= AppConfig.getDouble("vibration.spike.percent", 0.2)) {
              val alert = AlertRecord(
                finalMetrics.window_end, finalMetrics.machine_id,
                f"振动突增:fix.py{pct * 100}%.2f%%", finalMetrics.avg_vib_rms, 0.0, "请立即检查设备", new Timestamp(System.currentTimeMillis())
              )
              // 写入 Kafka
              val alertJson = "{\"machine_id\":\"" + alert.machine_id + "\",\"alert_time\":\"" + alert.alert_time + "\",\"alert_type\":\"" + alert.alert_type + "\",\"trigger_value\":" + alert.trigger_value + ",\"threshold_value\":" + alert.threshold_value + ",\"suggested_action\":\"" + alert.suggested_action + "\"}"
              jedis.rpush("temp_kafka_alerts", alertJson) // 使用 Redis 作为中转，或者直接发 Kafka
            }
          }
          
          prevVibRmsState.update(finalMetrics.avg_vib_rms)
          lastMetricsState.update(finalMetrics)
          out.collect(finalMetrics)
        }
        
        override def close(): Unit = if (jedis != null) jedis.close()
      })

    // 合并三个窗口流写入 ClickHouse realtime_status_window
    val allWindows = interpolated1m.union(window5m, window1h)
    allWindows.addSink(JdbcSink.sink(
      "INSERT INTO realtime_status_window (window_end, machine_id, max_temperature, avg_vib_rms) VALUES (?, ?, ?, ?)",
      new JdbcStatementBuilder[WindowMetrics] {
        override def accept(ps: PreparedStatement, t: WindowMetrics): Unit = {
          ps.setTimestamp(1, t.window_end)
          ps.setString(2, t.machine_id)
          ps.setDouble(3, t.max_temperature)
          ps.setDouble(4, t.avg_vib_rms)
        }
      },
      JdbcExecutionOptions.builder().withBatchSize(100).build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(ckUrl).withDriverName("ru.yandex.clickhouse.ClickHouseDriver").build()
    ))

    // ==========================================
    // 任务 16: 实时异常检测与报警
    // ==========================================
    val tempThreshold = AppConfig.getDouble("threshold.temperature", 80.0)
    val vibThreshold = AppConfig.getDouble("threshold.vibration", 2.0)

    val sensorAlerts = sensorStream.flatMap { s =>
      if (s.temperature > tempThreshold || s.vibration_x > vibThreshold) {
        val alertType = if (s.temperature > tempThreshold) "高温预警" else "振动异常"
        val triggerVal = if (s.temperature > tempThreshold) s.temperature else s.vibration_x
        val threshVal = if (s.temperature > tempThreshold) tempThreshold else vibThreshold
        Some(AlertRecord(s.timestamp, s.machine_id, alertType, triggerVal, threshVal, "请立即检查设备", new Timestamp(System.currentTimeMillis())))
      } else None
    }

    val logAlerts = logStream.flatMap { l =>
      if (l.error_code == "999") {
        Some(AlertRecord(l.timestamp, l.machine_id, "严重错误: 999", 999.0, 0.0, "请立即检查硬件日志", new Timestamp(System.currentTimeMillis())))
      } else None
    }

    val allAlerts = sensorAlerts.union(logAlerts)

    allAlerts.addSink(JdbcSink.sink(
      "INSERT INTO realtime_alerts (alert_time, machine_id, alert_type, trigger_value, threshold_value, suggested_action, insert_time) VALUES (?, ?, ?, ?, ?, ?, ?)",
      new JdbcStatementBuilder[AlertRecord] {
        override def accept(ps: PreparedStatement, t: AlertRecord): Unit = {
          ps.setTimestamp(1, t.alert_time)
          ps.setString(2, t.machine_id)
          ps.setString(3, t.alert_type)
          ps.setDouble(4, t.trigger_value)
          ps.setDouble(5, t.threshold_value)
          ps.setString(6, t.suggested_action)
          ps.setTimestamp(7, t.insert_time)
        }
      },
      JdbcExecutionOptions.builder().withBatchSize(10).build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(ckUrl).withDriverName("ru.yandex.clickhouse.ClickHouseDriver").build()
    ))

    // Alert 写入 Kafka alert_topic
    val kafkaAlertSink = new FlinkKafkaProducer[String](
      AppConfig.getString("kafka.alert_topic", "alert_topic"),
      new SimpleStringSchema(),
      kafkaProps
    )
    allAlerts.map(a => s"""{"machine_id":"src/main/scala/org/example/tasks/FlinkRealtimeEngine.scala{a.machine_id}","alert_time":"src/main/scala/org/example/tasks/FlinkRealtimeEngine.scala{a.alert_time}","alert_type":"src/main/scala/org/example/tasks/FlinkRealtimeEngine.scala{a.alert_type}","trigger_value":src/main/scala/org/example/tasks/FlinkRealtimeEngine.scala{a.trigger_value},"threshold_value":src/main/scala/org/example/tasks/FlinkRealtimeEngine.scala{a.threshold_value},"suggested_action":"src/main/scala/org/example/tasks/FlinkRealtimeEngine.scala{a.suggested_action}"}""")
      .addSink(kafkaAlertSink)

    // ==========================================
    // 任务 18: 实时工艺参数偏离监控
    // ==========================================
    val deviationStream = sensorStream.map(new RichMapFunction[SensorRaw, (String, Double, Double, Timestamp, Double, Double)] {
      var jedis: Jedis = _
      override def open(parameters: Configuration): Unit = {
        jedis = new Jedis(redisHost, redisPort)
      }
      override def map(s: SensorRaw): (String, Double, Double, Timestamp, Double, Double) = {
        val stdCurrentStr = jedis.hget(s"std_params:src/main/scala/org/example/tasks/FlinkRealtimeEngine.scala{s.machine_id}", "current")
        val stdSpeedStr = jedis.hget(s"std_params:src/main/scala/org/example/tasks/FlinkRealtimeEngine.scala{s.machine_id}", "speed")
        val stdCurrent = if (stdCurrentStr != null) stdCurrentStr.toDouble else 30.0
        val stdSpeed = if (stdSpeedStr != null) stdSpeedStr.toDouble else 3000.0
        
        val currentDev = math.abs(s.current - stdCurrent) / stdCurrent
        val speedDev = math.abs(s.speed - stdSpeed) / stdSpeed
        (s.machine_id, currentDev, speedDev, s.timestamp, s.current, stdCurrent)
      }
      override def close(): Unit = if (jedis != null) jedis.close()
    }).filter(x => x._2 > AppConfig.getDouble("deviation.threshold", 0.028) || x._3 > AppConfig.getDouble("deviation.threshold", 0.028))

    val aggDevStream = deviationStream
      .keyBy(_._1)
      .window(SlidingProcessingTimeWindows.of(Time.seconds(30), Time.seconds(10)))
      .process(new org.apache.flink.streaming.api.scala.function.ProcessWindowFunction[(String, Double, Double, Timestamp, Double, Double), ProcessDeviation, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Double, Double, Timestamp, Double, Double)], out: Collector[ProcessDeviation]): Unit = {
          var maxActual = Double.MinValue
          var maxStd = Double.MinValue
          var maxDev = Double.MinValue
          elements.foreach { e =>
            if (e._5 > maxActual) maxActual = e._5
            if (e._6 > maxStd) maxStd = e._6
            if (e._2 > maxDev) maxDev = e._2
          }
          if (maxActual != Double.MinValue) {
            out.collect(ProcessDeviation(
              new Timestamp(context.window.getStart),
              key, "current", maxActual, maxStd, maxDev * 100, 30
            ))
          }
        }
      })

    aggDevStream.addSink(JdbcSink.sink(
      "INSERT INTO realtime_process_deviation (record_time, machine_id, param_name, actual_value, standard_value, deviation_ratio, duration_seconds) VALUES (?, ?, ?, ?, ?, ?, ?)",
      new JdbcStatementBuilder[ProcessDeviation] {
        override def accept(ps: PreparedStatement, t: ProcessDeviation): Unit = {
          ps.setTimestamp(1, t.record_time)
          ps.setString(2, t.machine_id)
          ps.setString(3, t.param_name)
          ps.setDouble(4, t.actual_value)
          ps.setDouble(5, t.standard_value)
          ps.setDouble(6, t.deviation_ratio)
          ps.setInt(7, t.duration_seconds)
        }
      },
      JdbcExecutionOptions.builder().withBatchSize(100).build(),
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(ckUrl).withDriverName("ru.yandex.clickhouse.ClickHouseDriver").build()
    ))

    // ==========================================
    // 任务 13 & 14: 实时数据入湖 (Hudi Flink)
    // ==========================================
    tableEnv.createTemporaryView("sensor_raw", sensorStream)
    tableEnv.executeSql(s"""
      CREATE TABLE sensor_detail_realtime (
        machine_id STRING,
        ts BIGINT,
        temperature DOUBLE,
        vibration_x DOUBLE,
        vibration_y DOUBLE,
        vibration_z DOUBLE,
        current DOUBLE,
        noise DOUBLE,
        speed DOUBLE,
        PRIMARY KEY (machine_id, ts) NOT ENFORCED
      ) WITH (
        'connector' = 'hudi',
        'path' = '/hudi/dwd/sensor_detail_realtime',
        'table.type' = 'MERGE_ON_READ',
        'compaction.async.enabled' = 'true',
        'compaction.delta_commits' = '5'
      )
    """)
    tableEnv.executeSql("INSERT INTO sensor_detail_realtime SELECT machine_id, ts, temperature, vibration_x, vibration_y, vibration_z, current, noise, speed FROM sensor_raw")

    val enrichedLogStream = logStream.map { l =>
      val severity = if (l.error_code == "999") "Critical" else if (l.error_code == "500") "High" else "Normal"
      (l.machine_id, l.ts, l.error_code, l.error_msg, l.stack_trace, severity)
    }
    tableEnv.createTemporaryView("log_raw", enrichedLogStream)
    tableEnv.executeSql(s"""
      CREATE TABLE device_log_realtime (
        machine_id STRING,
        ts BIGINT,
        error_code STRING,
        error_msg STRING,
        stack_trace STRING,
        severity STRING,
        PRIMARY KEY (machine_id, ts) NOT ENFORCED
      ) WITH (
        'connector' = 'hudi',
        'path' = '/hudi/dwd/device_log_realtime',
        'table.type' = 'MERGE_ON_READ',
        'compaction.async.enabled' = 'true',
        'compaction.delta_commits' = '5'
      )
    """)
    tableEnv.executeSql("INSERT INTO device_log_realtime SELECT * FROM log_raw")

    env.execute("Flink Realtime Engine")
  }
}

```


#### Short summary: 

empty definition using pc, found symbol in pc: scala.