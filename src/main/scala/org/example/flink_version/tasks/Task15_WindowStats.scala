package org.example.flink_version.tasks

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import java.sql.PreparedStatement

case class WindowStatsResult(window_start: Long, window_end: Long, machine_id: String, max_temp: Double, vibration_rms_mean: Double)

/**
 * 任务组 5：实时指标与推理 (Flink -> ClickHouse)
 * 15. 实时状态窗口统计
 * 逻辑：1 分钟滚动窗口，计算各设备温度最大值、振动 RMS 均值。
 */
object Task15_WindowStats {

  def startWindowStatsSink(enrichedStream: DataStream[EnrichedSensorData], ckUrl: String): Unit = {
    
    val windowedStream = enrichedStream
      .keyBy(_.machine_id)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .aggregate(
        new org.apache.flink.api.common.functions.AggregateFunction[EnrichedSensorData, (Double, Double, Int), (Double, Double)] {
          override def createAccumulator(): (Double, Double, Int) = (0.0, 0.0, 0)
          override def add(value: EnrichedSensorData, acc: (Double, Double, Int)): (Double, Double, Int) = {
            (Math.max(acc._1, value.temperature), acc._2 + value.vibration_x, acc._3 + 1) // 简化RMS为X轴均值
          }
          override def getResult(acc: (Double, Double, Int)): (Double, Double) = (acc._1, if (acc._3 == 0) 0.0 else acc._2 / acc._3)
          override def merge(a: (Double, Double, Int), b: (Double, Double, Int)): (Double, Double, Int) = {
            (Math.max(a._1, b._1), a._2 + b._2, a._3 + b._3)
          }
        },
        new org.apache.flink.streaming.api.scala.function.WindowFunction[(Double, Double), WindowStatsResult, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow] {
          override def apply(key: String, window: org.apache.flink.streaming.api.windowing.windows.TimeWindow, input: Iterable[(Double, Double)], out: org.apache.flink.util.Collector[WindowStatsResult]): Unit = {
            val res = input.iterator.next()
            out.collect(WindowStatsResult(window.getStart, window.getEnd, key, res._1, res._2))
          }
        }
      )

    windowedStream.addSink(
      JdbcSink.sink(
        "INSERT INTO realtime_status_window (window_start, window_end, machine_id, max_temp, vibration_rms_mean) VALUES (?, ?, ?, ?, ?)",
        new JdbcStatementBuilder[WindowStatsResult] {
          override def accept(ps: PreparedStatement, t: WindowStatsResult): Unit = {
            ps.setLong(1, t.window_start)
            ps.setLong(2, t.window_end)
            ps.setString(3, t.machine_id)
            ps.setDouble(4, t.max_temp)
            ps.setDouble(5, t.vibration_rms_mean)
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