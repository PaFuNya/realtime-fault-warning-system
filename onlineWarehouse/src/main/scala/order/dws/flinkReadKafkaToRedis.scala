package order.dws

import org.apache.flink.streaming.api.scala._

object flinkReadKafkaToRedis {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
  }

}
