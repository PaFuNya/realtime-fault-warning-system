package order.ods

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.common.serialization.StringDeserializer


object flinkReadKafka {
  def main(args: Array[String]): Unit = {
    //获取flink的流式执行环境
    val env:StreamExecutionEnvironment=StreamExecutionEnvironment
      .getExecutionEnvironment
      env.setParallelism(1)
    // 设置kafka的数据源

    val kafkaSource: KafkaSource[String] = KafkaSource.builder()
      .setBootstrapServers("bigdata1:9092")
      .setTopics("order")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(classOf[StringDeserializer]))
      .build()

    //设置Flink的数据源为kafka,创建数据流
    val ds:DataStream[String]=env.fromSource(
      kafkaSource,WatermarkStrategy.noWatermarks[String](),"read kafka")

    //打印
    ds.print()
    //执行
    env.execute()
  }
}
