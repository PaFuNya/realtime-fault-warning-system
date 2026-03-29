package order.app

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.common.serialization.StringDeserializer

object orderConsumToRedis {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment=StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)

    val kafkaSource:KafkaSource[String]=KafkaSource.builder()
      .setBootstrapServers("master:9092")
      .setTopics("orderDetail")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(classOf[StringDeserializer]))
      .build()
    val ds:DataStream[String]=env.fromSource(
      kafkaSource,WatermarkStrategy.noWatermarks[String](),"read orderInfo"
    )
    val filter_ds:DataStream[String]=ds.filter(_.split(",").length!=1)
    val filter_ds2:DataStream[String]=filter_ds.filter(!_.split(",")(1).equals("\"id\""))
    case class ItemSummary(skuId: String,price: Double, quantity: Int)

    val itemSummary=filter_ds2.map(record=>{
        val fields=record.split(",")
        ItemSummary(fields(3).split("\"")(1),fields(6).split("\"")(1).toDouble,fields(7).split("\"")(1).toInt)
      })
      .map(detail => (detail.skuId, detail.price * detail.quantity))
      .keyBy(_._1)
      .sum(1)
    itemSummary.print()

    env.execute()
  }

}
