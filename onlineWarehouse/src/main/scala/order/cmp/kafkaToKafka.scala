package order.cmp

import order.ods.flinkReadKafkaToKafka.SplitToTopics
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.common.serialization.StringDeserializer

class kafkaToKafka {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaSource:KafkaSource[String]=KafkaSource.builder()
      .setBootstrapServers("master:9092")
      .setTopics("order")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(classOf[StringDeserializer]))
      .build()
    val ds:DataStream[String]=env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"read kafka")

    val orderInfoDag:OutputTag[String]=OutputTag[String]("orderInfo")
    val orderDetailDag:OutputTag[String]=OutputTag[String]("orderDetail")
    val sideOutputStream:DataStream[String]=ds.process(new SplitToTopics(orderInfoDag,orderDetailDag))

    val orderInfoProducer:KafkaSink[String]=KafkaSink.builder()
      .setBootstrapServers("master:9092")
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
      .setTopic("orderInfo")
      .setValueSerializationSchema(new SimpleStringSchema())
        .build())
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()

    sideOutputStream.getSideOutput(orderInfoDag).sinkTo(orderInfoProducer)
  }

}
//class SplitToTopics(or)