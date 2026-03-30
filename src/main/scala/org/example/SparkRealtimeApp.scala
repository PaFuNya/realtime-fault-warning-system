package org.example

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import redis.clients.jedis.Jedis
import java.sql.Timestamp

// 数据类型定义
case class SensorData(machine_id: String, ts: Long, temperature: Double, vibration_x: Double, vibration_y: Double, vibration_z: Double, current: Double, noise: Double, speed: Double, timestamp: Timestamp)
case class DeviceState(var lastTs: Long, var lastTemp: Double, var lastVibX: Double, var status: String, var healthScore: Int)
case class EnrichedSensor(machine_id: String, timestamp: Timestamp, temperature: Double, vibration_x: Double, current: Double, speed: Double, temp_slope: Double, vib_increase_ratio: Double, status: String, health_score: Int)

object SparkRealtimeApp {

  // 2.1 缺失值填补 与 2.2 状态机及特征计算
  def updateDeviceState(machineId: String, inputs: Iterator[SensorData], state: GroupState[DeviceState]): Iterator[EnrichedSensor] = {
    var currentState = if (state.exists) state.get else DeviceState(0L, 0.0, 0.0, "启动中", 100)
    val results = new scala.collection.mutable.ListBuffer[EnrichedSensor]()

    inputs.foreach { input =>
      // 2.1 填补缺失值 (前向插值，用上一次有效值替代 NaN 或极端异常缺失)
      val temp = if (input.temperature.isNaN) currentState.lastTemp else input.temperature
      val vibX = if (input.vibration_x.isNaN) currentState.lastVibX else input.vibration_x

      // 2.2 实时特征: 温度上升斜率, 振动突增率
      val timeDiff = if (currentState.lastTs > 0) (input.ts - currentState.lastTs) / 1000.0 else 0.0
      val tempSlope = if (timeDiff > 0) (temp - currentState.lastTemp) / timeDiff else 0.0
      val vibIncrease = if (currentState.lastVibX > 0) (vibX - currentState.lastVibX) / currentState.lastVibX else 0.0

      // 2.2 状态机识别 (根据转速判断)
      if (input.speed > 1000 && currentState.status == "启动中") currentState.status = "稳定运行"
      else if (input.speed < 100) currentState.status = "异常停机"

      // 健康分扣减模拟 (振动突增扣分)
      if (vibIncrease > 0.2) currentState.healthScore = Math.max(0, currentState.healthScore - 5)

      currentState.lastTs = input.ts
      currentState.lastTemp = temp
      currentState.lastVibX = vibX

      results += EnrichedSensor(machineId, input.timestamp, temp, vibX, input.current, input.speed, tempSlope, vibIncrease, currentState.status, currentState.healthScore)
    }
    state.update(currentState)
    results.iterator
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RealtimeDataProcess")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val kafkaBrokers = "100.126.226.67:9092,100.90.72.128:9092,100.123.80.25:9092"
    val ckUrl = "jdbc:clickhouse://127.0.0.1:8123/shtd_ads"
    val ckProperties = new java.util.Properties()
    ckProperties.put("user", "default")
    ckProperties.put("password", "123456")

    // Schema
    val sensorSchema = new StructType()
      .add("machine_id", StringType).add("ts", LongType).add("temperature", DoubleType)
      .add("vibration_x", DoubleType).add("vibration_y", DoubleType).add("vibration_z", DoubleType)
      .add("current", DoubleType).add("noise", DoubleType).add("speed", DoubleType)

    val logSchema = new StructType()
      .add("machine_id", StringType).add("ts", LongType).add("error_code", StringType)
      .add("error_msg", StringType).add("stack_trace", StringType)

    // Read Streams & 2.1 过滤乱序 (withWatermark)
    val sensorRawDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "sensor_raw")
      .option("startingOffsets", "latest").load()
      .select(from_json(col("value").cast("string"), sensorSchema).alias("data")).select("data.*")
      .withColumn("timestamp", timestamp_seconds(col("ts") / 1000))
      .withWatermark("timestamp", "5 seconds")

    val logRawDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBrokers)
      .option("subscribe", "log_raw")
      .option("startingOffsets", "latest").load()
      .select(from_json(col("value").cast("string"), logSchema).alias("data")).select("data.*")
      .withColumn("timestamp", timestamp_seconds(col("ts") / 1000))
      .withWatermark("timestamp", "5 seconds")

    // Task 13 & 14: Hudi Writes
    val hudiSensorOptions = Map("hoodie.table.name" -> "sensor_detail_realtime", "hoodie.datasource.write.recordkey.field" -> "machine_id,ts", "hoodie.datasource.write.precombine.field" -> "ts", "hoodie.datasource.write.table.type" -> "MERGE_ON_READ", "hoodie.datasource.write.operation" -> "upsert", "hoodie.compact.inline" -> "false", "hoodie.compact.async.enable" -> "true")
    sensorRawDF.writeStream.format("hudi").options(hudiSensorOptions).option("checkpointLocation", "/tmp/checkpoints/sensor_hudi").outputMode("append").start("file:///tmp/hudi_dwd/sensor_detail_realtime")

    val hudiLogOptions = Map("hoodie.table.name" -> "device_log_realtime", "hoodie.datasource.write.recordkey.field" -> "machine_id,ts", "hoodie.datasource.write.precombine.field" -> "ts", "hoodie.datasource.write.table.type" -> "MERGE_ON_READ", "hoodie.datasource.write.operation" -> "upsert", "hoodie.compact.inline" -> "false", "hoodie.compact.async.enable" -> "true")
    logRawDF.withColumn("severity", when(col("error_code") === "999", "Critical").when(col("error_code") === "500", "High").otherwise("Normal"))
      .writeStream.format("hudi").options(hudiLogOptions).option("checkpointLocation", "/tmp/checkpoints/log_hudi").outputMode("append").start("file:///tmp/hudi_dwd/device_log_realtime")

    // 2.2 状态机与特征计算 (flatMapGroupsWithState)
    val sensorDS = sensorRawDF.as[SensorData]
    val enrichedSensorDS = sensorDS
      .groupByKey(_.machine_id)
      .flatMapGroupsWithState(org.apache.spark.sql.streaming.OutputMode.Append(), GroupStateTimeout.NoTimeout())(updateDeviceState)
    val enrichedDF = enrichedSensorDS.toDF()

    // 2.4 状态快照：每分钟更新 Redis 健康分 & 写入 ClickHouse device_realtime_status
    enrichedDF.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      batchDF.select("machine_id", "health_score", "status").rdd.foreachPartition { partition =>
        val jedis = new Jedis("127.0.0.1", 6379)
        partition.foreach { row =>
          val mid = row.getAs[String]("machine_id")
          jedis.hset(s"device_status:$mid", "health_score", row.getAs[Int]("health_score").toString)
          jedis.hset(s"device_status:$mid", "status", row.getAs[String]("status"))
        }
        jedis.close()
      }
      try {
        batchDF.write.mode("append").jdbc(ckUrl, "device_realtime_status", ckProperties)
      } catch { case _: Exception => }
    }.option("checkpointLocation", "/tmp/checkpoints/redis_status").start()

    // Task 15: 实时状态窗口统计
    enrichedDF.withWatermark("timestamp", "2 minutes")
      .groupBy(window(col("timestamp"), "1 minute"), col("machine_id"))
      .agg(max("temperature").alias("max_temp"), mean(col("vibration_x")).alias("vibration_rms_mean"))
      .select(col("window.start").alias("window_start"), col("window.end").alias("window_end"), col("machine_id"), col("max_temp"), col("vibration_rms_mean"))
      .writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        try { batchDF.write.mode("append").jdbc(ckUrl, "realtime_status_window", ckProperties) } catch { case _: Exception => }
      }.option("checkpointLocation", "/tmp/checkpoints/status_window").start()

    // Task 16: 实时异常检测与报警 (sensor_raw + log_raw) & 2.4 推送 Kafka alert_topic
    val sensorAlerts = enrichedDF.filter(col("temperature") > 80.0 || col("vib_increase_ratio") > 0.2)
      .select(col("timestamp").alias("alert_time"), col("machine_id"),
        when(col("temperature") > 80.0, "High Temperature").otherwise("Vibration Sudden Increase > 20%").alias("alert_type"),
        lit("Inspect machine immediately").alias("suggested_action"))

    val logAlerts = logRawDF.filter(col("error_code") === "999")
      .select(col("timestamp").alias("alert_time"), col("machine_id"),
        lit("Critical Log Error 999").alias("alert_type"),
        lit("Check hardware logs").alias("suggested_action"))

    val combinedAlerts = sensorAlerts.union(logAlerts)

    combinedAlerts.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      try { batchDF.write.mode("append").jdbc(ckUrl, "realtime_alerts", ckProperties) } catch { case _: Exception => }
    }.option("checkpointLocation", "/tmp/checkpoints/realtime_alerts_ck").start()

    combinedAlerts.selectExpr("CAST(machine_id AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("topic", "alert_topic")
      .option("checkpointLocation", "/tmp/checkpoints/realtime_alerts_kafka").start()

    // Task 18: 实时工艺参数偏离监控 (从 Redis 读取标准工艺参数)
    enrichedDF.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      val sparkSession = batchDF.sparkSession
      import sparkSession.implicits._
      
      // 使用 mapPartitions 连接 Redis 获取该设备的标准参数并对比
      val devDF = batchDF.mapPartitions { iter =>
        val jedis = new Jedis("127.0.0.1", 6379)
        val res = iter.map { row =>
          val mid = row.getAs[String]("machine_id")
          val current = row.getAs[Double]("current")
          val speed = row.getAs[Double]("speed")
          
          val stdCurrent = Option(jedis.hget(s"std_params:$mid", "current")).map(_.toDouble).getOrElse(30.0)
          val stdSpeed = Option(jedis.hget(s"std_params:$mid", "speed")).map(_.toDouble).getOrElse(3000.0)
          
          val currentDev = Math.abs(current - stdCurrent) / stdCurrent
          val speedDev = Math.abs(speed - stdSpeed) / stdSpeed
          (mid, currentDev, speedDev, row.getAs[Timestamp]("timestamp"))
        }.filter(x => x._2 > 0.1 || x._3 > 0.1) // 偏离阈值 > 10%
        jedis.close()
        res
      }.toDF("machine_id", "current_dev", "speed_dev", "timestamp")

      devDF.createOrReplaceTempView("temp_dev")
      // 统计 30 秒内偏离记录数
      val aggDevDF = sparkSession.sql("""
        SELECT machine_id, window.start as start_time, window.end as end_time,
               MAX(current_dev) as max_current_dev, MAX(speed_dev) as max_speed_dev
        FROM (
          SELECT *, window(timestamp, '30 seconds') as window FROM temp_dev
        )
        GROUP BY machine_id, window
        HAVING count(*) >= 5
      """)

      try { aggDevDF.write.mode("append").jdbc(ckUrl, "realtime_process_deviation", ckProperties) } catch { case _: Exception => }
    }.option("checkpointLocation", "/tmp/checkpoints/process_dev").start()

    spark.streams.awaitAnyTermination()
  }
}