package order.app

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.kafka.common.serialization.StringDeserializer

object orderCountToRedis {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment=StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)

    val kafkaSource:KafkaSource[String]=KafkaSource.builder()
      .setBootstrapServers("master:9092")
      .setTopics("orderInfo")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(classOf[StringDeserializer]))
      .build()

    val ds:DataStream[String]=env.fromSource(
      kafkaSource,WatermarkStrategy.noWatermarks[String](),"read orderInfo"
    )

    val filter_ds:DataStream[String]=ds.filter(_.split(",").length!=1)
    val filter_ds2:DataStream[String]=filter_ds.filter(!_.split(",")(1).equals("\"id\""))

    //数据处理orderInfo为事件时间数据
    val orderCount:DataStream[(String,Int)]=filter_ds2.map(record=>{
      val fields=record.split(",")
      val orderStatus=fields(5).split("\"")(1).toInt

      val count=if(List(1001,1002,1004,1005).contains(orderStatus)) 1 else 0
      ("totalcount",count)
      })
      .keyBy(_._1)
      .reduce((current,next)=>("totalcount",current._2+next._2))
    orderCount.print()
    orderCount.addSink(new RedisSink[(String,Int)](flinkJedisPoolConfig,new MyRedisMapper))
   env.execute()
  }
  class MyRedisMapper extends RedisMapper[Tuple2[String,Int]]{

    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

    override def getKeyFromData(t: (String, Int)): String = t._1

    override def getValueFromData(t: (String,Int)): String = t._2.toString
  }
  private  val flinkJedisPoolConfig:FlinkJedisPoolConfig=new FlinkJedisPoolConfig.Builder()
    .setHost("192.168.1.100")
    .setPort(6379)
    .build()
}


