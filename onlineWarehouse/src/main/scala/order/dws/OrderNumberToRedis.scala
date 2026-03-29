package order.dws


import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper._
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

import java.text.SimpleDateFormat
import java.time.Duration

object OrderNumberToRedis {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment=StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)
     //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val kafkaSource:KafkaSource[String]=KafkaSource.builder()
      .setBootstrapServers("master:9092")
      .setTopics("orderInfo")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(classOf[StringDeserializer]))
      .build()
    val ds:DataStream[String]=env.fromSource(
      kafkaSource,
      WatermarkStrategy
        .forBoundedOutOfOrderness[String](Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[String] {
          override def extractTimestamp(element: String, recordTimestamp: Long): Long = {
            // 假设你的事件时间是消息的一部分，你需要在这里解析它
            val fields = element.split(",")
            val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val createTime = formatter.parse(fields(11).split("\"")(1)).getTime
            val operateTime = if (fields(12).nonEmpty) formatter.parse(fields(12).split("\"")(1)).getTime else createTime
            Math.max(createTime, operateTime)
          }
        }),"read orderInfo"
    )

    //ds.print()
    val filter_ds:DataStream[String]=ds.filter(_.split(",").length!=1)
    val filter_ds2:DataStream[String]=filter_ds.filter(!_.split(",")(1).equals("\"id\""))
    //filter_ds2.print()

    //定义事件类，做好事件时间预处理的
    //case class OrderEvent(orderId:String,orderStatus:String,eventTime:Long)
    //数据处理orderInfo为事件时间数据
    val orderEvents=filter_ds2.map(record=>{
      val fields=record.split(",")
      val order_id=fields(1)
      val order_status=fields(5).split("\"")(1)
      //OrderEvent(fields(1),fields(5))
        (order_id,order_status)
    })
      //.assignAscendingTimestamps(_.eventTime)
      //.assignTimestampsAndWatermarks(ws) // 应用Watermark策略
      .filter(order=>List("1001","1002","1004","1005").contains(order._2))

    orderEvents.print()
    //处理滚动时间窗口的数据统计
    /*val orderCounts:DataStream[(String,Int)]=orderEvents
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(5))
      .process(new ProcessAllWindowFunction[OrderEvent,(String,Long),TimeWindow] {

        override def process(context: Context, elements: Iterable[OrderEvent],
                             out: Collector[(String,Long)]): Unit = {
          val totalCount=elements.size
          out.collect("totalcount",totalCount.toLong)
        }
      })*/
//    orderCounts.print()
//    //存入redis
//    orderCounts.addSink(new RedisSink[(String,Int)](flinkJedisPoolConfig,new MyRedisMapper))
    env.execute()
  }
  class MyRedisMapper extends RedisMapper[Tuple2[String,Int]]{

    override def getCommandDescription: RedisCommandDescription = new RedisCommandDescription(RedisCommand.SET)

    override def getKeyFromData(t: (String, Int)): String = t._1

    override def getValueFromData(t: (String, Int)): String = t._2.toString
  }
  private  val flinkJedisPoolConfig:FlinkJedisPoolConfig=new FlinkJedisPoolConfig.Builder()
    .setHost("192.168.1.100")
    .setPort(6379)
    .build()
}


