package order.ods

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

object flinkReadKafkaToKafka {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaSource:KafkaSource[String]=KafkaSource.builder()
      .setBootstrapServers("bigdata1:9092")
      .setTopics("order")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(classOf[StringDeserializer]))
      .build()
    val ds:DataStream[String]=env
      .fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"kafka Source")
    //定义侧流标签
    val orderInfoDag:OutputTag[String]=OutputTag[String]("orderInfo")
    val orderDetailDag:OutputTag[String]=OutputTag[String]("orderDetails")

    //数据分发侧流处理
    val sideOutputStream:DataStream[String]=ds.process(new SplitToTopics(orderInfoDag,orderDetailDag))

    //输出到topic(order_info)的Producer
    val orderInfoProducer:KafkaSink[String]=KafkaSink.builder()
      .setBootstrapServers("bigdata1:9092")
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
      .setTopic("orderInfo")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build())
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()
    val orderDetailProducer:KafkaSink[String]=KafkaSink.builder()
      .setBootstrapServers("bigdata1:9092")
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        .setTopic("orderDetails")
        .setValueSerializationSchema(new SimpleStringSchema())
        .build())
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()

    //根据标签分发
    sideOutputStream.getSideOutput(orderInfoDag).sinkTo(orderInfoProducer)
    sideOutputStream.getSideOutput(orderDetailDag).sinkTo(orderDetailProducer)


    env.execute("read kafka")

  }
  //分流贴标签
  class SplitToTopics(orderInfoDag: OutputTag[String], orderDetailDag: OutputTag[String]) extends
  ProcessFunction[String,String]{
    def isOrderInfo(value:String):Boolean={
      val strings:Array[String]=value.split(",")
      strings(0)=="I"
    }
    def isOrderDetail(value:String):Boolean={
      val strings:Array[String]=value.split(",")
      strings(0)=="D"
    }
    //贴标签
    override def processElement(value: String,
                                ctx: ProcessFunction[String, String]#Context,
                                out: Collector[String]): Unit = {
      if(isOrderInfo(value)){
        ctx.output(orderInfoDag,value)
      }else if(isOrderDetail(value)){
        ctx.output(orderDetailDag,value)
      }
    }
  }

}
