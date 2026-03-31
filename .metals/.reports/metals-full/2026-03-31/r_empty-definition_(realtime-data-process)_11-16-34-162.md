file:///D:/Desktop/Match/MATCH/OverMatch/src/main/scala/org/example/flink_version/tasks/Task18_ProcessDeviation.scala
empty definition using pc, found symbol in pc: 
semanticdb not found
empty definition using fallback
non-local guesses:
	 -org/apache/flink/streaming/api/scala/stdSpeed.
	 -org/apache/flink/streaming/api/scala/stdSpeed#
	 -org/apache/flink/streaming/api/scala/stdSpeed().
	 -stdSpeed.
	 -stdSpeed#
	 -stdSpeed().
	 -scala/Predef.stdSpeed.
	 -scala/Predef.stdSpeed#
	 -scala/Predef.stdSpeed().
offset: 2159
uri: file:///D:/Desktop/Match/MATCH/OverMatch/src/main/scala/org/example/flink_version/tasks/Task18_ProcessDeviation.scala
text:
```scala
package org.example.flink_version.tasks

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import java.sql.PreparedStatement
import redis.clients.jedis.Jedis

case class DeviationAlert(machine_id: String, start_time: Long, current_dev: Double, speed_dev: Double)

/**
 * 任务组 5：实时指标与推理 (Flink -> ClickHouse)
 * 18. 实时工艺参数偏离监控
 * 逻辑：对比实时电流/转速与标准值的偏差，持续偏离 30 秒即记录。
 */
object Task18_ProcessDeviation {

  class DeviationProcessFunction extends KeyedProcessFunction[String, EnrichedSensorData, DeviationAlert] {
    lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))
    lazy val maxCurrentDevState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("max-curr-dev", classOf[Double]))
    lazy val maxSpeedDevState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("max-speed-dev", classOf[Double]))

    @transient var jedis: Jedis = _

    override def open(parameters: org.apache.flink.configuration.Configuration): Unit = {
      jedis = new Jedis("127.0.0.1", 6379)
    }

    override def close(): Unit = {
      if (jedis != null) jedis.close()
    }

    override def processElement(value: EnrichedSensorData, ctx: KeyedProcessFunction[String, EnrichedSensorData, DeviationAlert]#Context, out: Collector[DeviationAlert]): Unit = {
      val mid = value.machine_id
      
      // 1. 读取 Redis 获取该设备的标准参数
      val stdCurrentStr = jedis.hget(s"std_params:$mid", "current")
      val stdSpeedStr = jedis.hget(s"std_params:$mid", "speed")
      val stdCurrent = if (stdCurrentStr != null) stdCurrentStr.toDouble else 30.0
      val stdSpeed = if (stdSpeedStr != null) stdSpeedStr.toDouble else 3000.0
      
      val currentDev = Math.abs(value.current - stdCurrent) / stdCurrent
      val speedDev = Math.abs(value.speed - stdSpeed) / stdSpeed@@
      
      val timerTs = timerTsState.value()
      
      // 2. 如果发生偏离 (> 10%)
      if (currentDev > 0.1 || speedDev > 0.1) {
        // 更新最大偏离值
        val currentMaxC = if (maxCurrentDevState.value() == 0.0) currentDev else Math.max(maxCurrentDevState.value(), currentDev)
        val currentMaxS = if (maxSpeedDevState.value() == 0.0) speedDev else Math.max(maxSpeedDevState.value(), speedDev)
        maxCurrentDevState.update(currentMaxC)
        maxSpeedDevState.update(currentMaxS)

        if (timerTs == 0L) {
          // 第一次偏离，注册 30 秒后的定时器
          val targetTs = value.ts + 30 * 1000L
          ctx.timerService().registerEventTimeTimer(targetTs)
          timerTsState.update(targetTs)
        }
      } else {
        // 恢复正常，清除定时器和状态
        if (timerTs > 0L) {
          ctx.timerService().deleteEventTimeTimer(timerTs)
          timerTsState.clear()
          maxCurrentDevState.clear()
          maxSpeedDevState.clear()
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, EnrichedSensorData, DeviationAlert]#OnTimerContext, out: Collector[DeviationAlert]): Unit = {
      val machineId = ctx.getCurrentKey
      val maxC = maxCurrentDevState.value()
      val maxS = maxSpeedDevState.value()
      
      // 触发报警
      out.collect(DeviationAlert(machineId, timestamp - 30000L, maxC, maxS))
      
      // 清理状态
      timerTsState.clear()
      maxCurrentDevState.clear()
      maxSpeedDevState.clear()
    }
  }

  def startDeviationSink(enrichedStream: DataStream[EnrichedSensorData], ckUrl: String): Unit = {
    val deviationAlerts = enrichedStream
      .keyBy(_.machine_id)
      .process(new DeviationProcessFunction)

    deviationAlerts.addSink(
      JdbcSink.sink(
        "INSERT INTO realtime_process_deviation (machine_id, start_time, max_current_dev, max_speed_dev) VALUES (?, ?, ?, ?)",
        new JdbcStatementBuilder[DeviationAlert] {
          override def accept(ps: PreparedStatement, t: DeviationAlert): Unit = {
            ps.setString(1, t.machine_id)
            ps.setLong(2, t.start_time)
            ps.setDouble(3, t.current_dev)
            ps.setDouble(4, t.speed_dev)
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
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 