package order.app

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._

import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

// 定义一个类来抽象商品销售汇总：商品ID，销售数量和销售额
case class sku(skuId:String,skuNum:Int)
object orderSumToRedis {
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


    class Top3ItemsProcessFunction extends ProcessFunction[(String, Int), (String,String)] {
      private var top3: List[(String, Int)] = List()

      override def processElement(value: (String, Int), ctx: ProcessFunction[(String, Int), (String, String)]#Context, out: Collector[(String, String)]): Unit = {
        top3 = (value :: top3)
          .sortBy(-_._2) // 按销售数量降序排序
          .take(3) // 取前三个商品
        val result = top3.map { case (itemId, amount) => s"$itemId:$amount" }.mkString(", ")
        out.collect("top3itemamount",result)
      }
    }
    //数据处理orderInfo为事件时间数据
    // 定义一个类来抽象商品销售汇总：商品ID，销售数量和销售额

    val itemSummary=filter_ds2.map(record=>{
        val fields=record.split(",")
        val sku_id=fields(3).split("\"")(1)
        val sku_num=fields(7).split("\"")(1).toInt
        (sku_id,sku_num)
      })
      .keyBy(_._1)
      .sum(1) // 对每个 SKU 的数量求和
      .process(new Top3ItemsProcessFunction)
      .uid("top3ItemsProcessFunction")

    itemSummary.print()
    itemSummary.addSink(new RedisSink[(String,String)](flinkJedisPoolConfig,new MyRedisMapper))
    env.execute()
  }
  class MyRedisMapper extends RedisMapper[Tuple2[String,String]]{

    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

    override def getKeyFromData(t: (String, String)): String = t._1

    override def getValueFromData(t: (String, String)): String = t._2.toString
  }
  private  val flinkJedisPoolConfig:FlinkJedisPoolConfig=new FlinkJedisPoolConfig.Builder()
    .setHost("192.168.1.100")
    .setPort(6379)
    .build()

}
