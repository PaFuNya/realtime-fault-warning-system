package org.example.flink_version.tasks

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import java.sql.PreparedStatement

/**
 * 模块二：实时数据处理引擎
 * 2.4 实时结果输出
 * 状态快照：每分钟更新 Redis 中的设备最新健康分。
 * 实时指标：写入 ClickHouse device_realtime_status 表。
 */
object Task2_4_RealtimeOutput {

  def startSinks(enrichedStream: DataStream[EnrichedSensorData], ckUrl: String): Unit = {
    
    // 1. 写入 Redis (每分钟更新设备最新健康分和状态)
    val jedisConfig = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build()

    // 写入健康分
    enrichedStream.addSink(new RedisSink[EnrichedSensorData](jedisConfig, new RedisMapper[EnrichedSensorData] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "device_health")
      override def getKeyFromData(data: EnrichedSensorData): String = data.machine_id
      override def getValueFromData(data: EnrichedSensorData): String = data.health_score.toString
    }))

    // 写入状态
    enrichedStream.addSink(new RedisSink[EnrichedSensorData](jedisConfig, new RedisMapper[EnrichedSensorData] {
      override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.HSET, "device_status")
      override def getKeyFromData(data: EnrichedSensorData): String = data.machine_id
      override def getValueFromData(data: EnrichedSensorData): String = data.status
    }))

    // 2. 写入 ClickHouse device_realtime_status
    enrichedStream.addSink(
      JdbcSink.sink(
        "INSERT INTO device_realtime_status (machine_id, status, health_score) VALUES (?, ?, ?)",
        new JdbcStatementBuilder[EnrichedSensorData] {
          override def accept(ps: PreparedStatement, t: EnrichedSensorData): Unit = {
            ps.setString(1, t.machine_id)
            ps.setString(2, t.status)
            ps.setInt(3, t.health_score)
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