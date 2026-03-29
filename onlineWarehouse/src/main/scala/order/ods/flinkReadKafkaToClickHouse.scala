package order.ods

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import java.sql.{Connection, DriverManager, PreparedStatement}

object flinkReadKafkaToClickHouse {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaSource: KafkaSource[String] = KafkaSource.builder()
      .setBootstrapServers("bigdata1:9092")
      .setTopics("orderDetails")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(classOf[StringDeserializer]))
      .build()
    val ds: DataStream[String] = env
      .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka Source")
    val filter_ds: DataStream[String] = ds.filter(_.split(",").length != 1)
    val filter_ds2: DataStream[String] = filter_ds.filter(!_.split(",")(1).equals("\"id\""))
    //filter_ds2.print()
    val map_ds = filter_ds2.map(line => {
      val words: Array[String] = line.split(",")
      val sku_price: Double = words(6).split("\"")(1).toDouble
      val sku_id: String = words(3).split("\"")(1)
      (sku_id, sku_price)
    }).keyBy(_._1).sum(1)
    map_ds.print()

    // 将数据写入 ClickHouse
    map_ds.addSink(new ClickHouseSink)

    env.execute("read kafka")
  }

  private class ClickHouseSink extends RichSinkFunction[(String, Double)] {
    private var connection: Connection = _
    private var preparedStatement: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      val url = "jdbc:clickhouse://bigdata1:8123/order"
      val driver = "com.clickhouse.jdbc.ClickHouseDriver" // 修复：使用 com.clickhouse.jdbc.ClickHouseDriver
      val user = "default"
      val password = ""

      Class.forName(driver)
      connection = DriverManager.getConnection(url, user, password)
      val sql = "INSERT INTO sku_prices (sku_id, sku_price) VALUES (?, ?)"
      preparedStatement = connection.prepareStatement(sql)
    }

    override def invoke(value: (String, Double)): Unit = {
      preparedStatement.setString(1, value._1)
      preparedStatement.setDouble(2, value._2)
      preparedStatement.executeUpdate()
    }

    override def close(): Unit = {
      if (preparedStatement != null) {
        preparedStatement.close()
      }
      if (connection != null) {
        connection.close()
      }
    }
  }
}
