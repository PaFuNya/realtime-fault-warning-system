package order.dws

import order.app
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringDeserializer

import java.text.SimpleDateFormat

object orderCountTopNToRedis {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment=StreamExecutionEnvironment
      .getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

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

    //数据处理orderInfo为事件时间数据
   /* val itemSummary:DataStream[app.ItemSummary]=filter_ds2.map(record=>{
        val fields=record.split(",")
        val formatter=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val createTime=formatter.parse(fields(8).split("\"")(1)).getTime
        app.ItemSummary(fields(3).split("\"")(1),fields(6).split("\"")(1).toDouble,fields(7).split("\"")(1).toInt,createTime)
      })*/
      //.assignAscendingTimestamps(_.eventTime)
    //itemSummary.print()
    // 简化，只展示销售额前三的逻辑，销售量类似
    /*val itemConsumptionSummary= itemSummary
      .map(detail => (detail.skuId, detail.quantity))
      .keyBy(_._1)
      //.sum(1)
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(20)))
      .allowedLateness(Time.seconds(5))
      .sum(1)
      .process(new ProcessAllWindowFunction[(String, Int),(String,String),TimeWindow] {
        def process(context: Context, elements: Iterable[(String, Int)],
                    out: Collector[(String, String)]): Unit = {
          val sortedItems = elements.toList.sortWith((x, y) => x._2 > y._2).take(3) // 根据销售额进行降序排序并取前N个
          val top3=sortedItems.map(x => s"${x._1}:${x._2}").mkString("[", ",", "]")
          out.collect("top3itemamout", top3)
        }
      })
    itemConsumptionSummary.print()
    itemConsumptionSummary.addSink(new RedisSink[(String,String)](flinkJedisPoolConfig,new MyRedisMapper))*/
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
// 定义一个类来抽象商品销售汇总：商品ID，销售数量和销售额
case class ItemSummary(skuId: String,price: Double, quantity: Int, eventTime:Long)

