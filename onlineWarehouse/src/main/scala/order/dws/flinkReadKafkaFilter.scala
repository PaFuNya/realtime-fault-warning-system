package order.dws

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


import java.util.Properties


object flinkReadKafkaFilter {


  def main(args: Array[String]): Unit = {

    // 获取flink的流式执行环境

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(2) // 设置代码并行度为1


    // 设置kafka的数据源

    val kafkaSource: KafkaSource[String] = KafkaSource.builder()

      .setBootstrapServers("master:9092")

      .setTopics("order")

      .setStartingOffsets(OffsetsInitializer.earliest())

      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(classOf[StringDeserializer]))

      .build()


    // 从kafkasource创建dataStream

    val ds: DataStream[String] = environment.

      fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka Source")


    // 打印从kafka获取到的数据

    //    ds.print()


    // 定义侧输出流标签

    val orderInfoDag: OutputTag[String] = OutputTag[String]("order_info")

    val orderDetailDag: OutputTag[String] = OutputTag[String]("order_detail")


    // 根据不同的数据分发到不同的侧输出流

    val sideOutputStream: DataStream[String] = ds.process(new SplitToTopics(orderInfoDag, orderDetailDag))


    // 定义输出到不同kafka的topic中

    // 输出数据到order_info的topic的Producer

    val orderInfoProducer: KafkaSink[String] = KafkaSink.builder()

      .setBootstrapServers("master:9092")

      .setRecordSerializer(KafkaRecordSerializationSchema.builder()

        .setTopic("orderInfo")

        .setValueSerializationSchema(new SimpleStringSchema())

        .build())

      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)

      .build()


    // 输出数据到order_detail的topic的Producer

    val orderDetailProducer: KafkaSink[String] = KafkaSink.builder()

      .setBootstrapServers("master:9092")

      .setRecordSerializer(KafkaRecordSerializationSchema.builder()

        .setTopic("orderDetail")

        .setValueSerializationSchema(new SimpleStringSchema())

        .build())

      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)

      .build()


    sideOutputStream.getSideOutput(orderInfoDag).sinkTo(orderInfoProducer)

    sideOutputStream.getSideOutput(orderDetailDag).sinkTo(orderDetailProducer)


    // 执行流式环境

    environment.execute("read kafka")

  }


  /**

   *  根据订单首字母不同 将数据分为 OrderInfo 和 OrderDetail

   * @param orderInfoDag orderInfo的dag

   * @param orderDetailDag orderDetail的dag

   */

  class SplitToTopics(orderInfoDag: OutputTag[String], orderDetailDag: OutputTag[String]) extends ProcessFunction[String,String] {


    def isOrderInfo(value: String): Boolean = {

      // 根据订单首字母区分是不是订单信息（OrderInfo）

      val strings: Array[String] = value.split(",")

      strings(0) == "I"

    }


    def isOrderDetail(value: String): Boolean = {

      // 根据订单首字母区分是不是订单信息（OrderInfo）

      val strings: Array[String] = value.split(",")

      strings(0) == "D"

    }


    override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {

      // 根据数据第一位判断是订单主表（OrderInfo）还是订单明细表（OrderDetail）,分发到不同的侧输出流

      if (isOrderInfo(value)) {

        ctx.output(orderInfoDag, value)

      } else if (isOrderDetail(value)) {

        ctx.output(orderDetailDag, value)

      }

    }

  }

}