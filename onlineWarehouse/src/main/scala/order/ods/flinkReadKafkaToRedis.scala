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

object flinkReadKafkaToRedis {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val kafkaSource:KafkaSource[String]=KafkaSource.builder()
      .setBootstrapServers("master:9092")
      .setTopics("orderDetail")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(classOf[StringDeserializer]))
      .build()
    val ds:DataStream[String]=env
      .fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"kafka Source")
    val filter_ds:DataStream[String]=ds.filter(_.split(",").length!=1)
    val filter_ds2:DataStream[String]=filter_ds.filter(!_.split(",")(1).equals("\"id\""))
    //filter_ds2.print()
    val map_ds=filter_ds2.map(line=>{
      val words:Array[String]=line.split(",")
      val sku_price:Double=words(6).split("\"")(1).toDouble
      val sku_id:String=words(3).split("\"")(1)
      (sku_id,sku_price)
    }).keyBy(_._1).sum(1)
    map_ds.print()

    //将数据写入redis
    map_ds.addSink(new RedisSink[(String,Double)](flinkJedisPoolConfig,new MyRedisMapper))


    env.execute("read kafka")
  }
  class MyRedisMapper extends RedisMapper[Tuple2[String,Double]]{

    override def getCommandDescription: RedisCommandDescription
    = new RedisCommandDescription(RedisCommand.SET)

    override def getKeyFromData(t: (String, Double)): String
    = t._1

    override def getValueFromData(t: (String, Double)): String = t._2.toString
  }
  private val flinkJedisPoolConfig:FlinkJedisPoolConfig=new FlinkJedisPoolConfig.Builder()
    .setHost("192.168.1.100")
    .setPort(6379)
    .build()
}
