package com.shtd.etl

import com.shtd.udf.SensorFeatureUDF
import com.shtd.util.ConfigUtil
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.config.HoodieWriteConfig.TBL_NAME
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object IndustryMain {
  def main(args: Array[String]): Unit = {
    // 加载配置文件（自动从 classpath 加载 config.properties）
    ConfigUtil.loadProperties()
    val hdfsUrl=ConfigUtil.getString("hdfs.url")
    val mysqlUrl=ConfigUtil.getString("mysql.url")
    val mysqlUser=ConfigUtil.getString("mysql.user")
    val mysqlPass=ConfigUtil.getString("mysql.pass")
    // 1. 初始化 Spark Session
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .appName("SHTD-Industry-Feature-Engineering")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // 2. 注册 UDF
    // 将我们定义的 SensorFeatureUDF 注册到 Spark SQL 上下文中
    spark.udf.register("extract_features", SensorFeatureUDF, SensorFeatureUDF.schema)

    // 3. 读取 MySQL 数据
    // 对应 shtd_industry2.sql 中的 EnvironmentData 表
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", mysqlUrl)
      .option("dbtable", "EnvironmentData")
      .option("user", mysqlUser)       // 请替换为你的数据库用户名
      .option("password", mysqlPass) // 请替换为你的数据库密码
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    // 4. 数据预处理与特征工程
    // 逻辑：按设备分组 -> 按时间窗口聚合 -> 收集温度数组 -> 调用 UDF 计算特征
    // 先过滤掉 BaseID 为 null 的源数据
    val filteredJdbcDF = jdbcDF
      .filter($"BaseID".isNotNull)
      .filter($"InPutTime".isNotNull)
      .filter($"BaseID" =!= "") // 同时过滤空字符串

    println(s"源数据行数：${jdbcDF.count()}, 过滤后行数：${filteredJdbcDF.count()}")

    val resultDF = filteredJdbcDF
      // 将字符串时间转为时间戳，用于窗口分组
      //.withColumn("time_ts", to_timestamp($"InPutTime", "yyyy-MM-dd HH:mm:ss"))
      .withColumn("time_ts", to_timestamp($"InPutTime", "yyyy-MM-dd HH:mm:ss.SSS"))
      // 过滤掉时间格式错误的数据
      .filter($"time_ts".isNotNull)
      // 设置水印（可选，用于处理乱序数据）
      .withWatermark("time_ts", "10 minutes")

      // 分组：按 设备ID(BaseID) 和 5分钟的时间窗口 分组
      .groupBy(
        window($"time_ts", "5 minutes"),
        $"BaseID"
      )
      // 聚合：收集该窗口内所有的 Temperature 字符串到一个数组中
      .agg(
        collect_list($"Temperature").alias("temp_sequence")
      )
      // 核心：调用 UDF 计算特征
      .withColumn("features", expr("extract_features(temp_sequence)"))

      // 展开特征结构体，方便查看
      .select(
        $"BaseID",
        $"window.start".alias("window_start"),
        $"window.end".alias("window_end"),
        $"features.mean".alias("avg_temp"),
        $"features.variance".alias("var_temp"),
        $"features.kurtosis".alias("kurtosis_temp"),
        $"features.fft_peak".alias("fft_peak")
      )
      // 过滤掉计算失败（特征为 null）的行
      .filter($"kurtosis_temp".isNotNull)
      // 关键：在这里立即过滤掉 window_end 为 null 的行
      .filter($"window_end".isNotNull && $"BaseID".isNotNull)

    // 5. 输出结果
    // 方案 A: 打印到控制台 (调试用)
    resultDF.show(false)

    // 严格检查 null 值
    val baseIdNullCount = resultDF.filter($"BaseID".isNull).count()
    val windowEndNullCount = resultDF.filter($"window_end".isNull).count()
    println(s"BaseID 为 null 的数量：$baseIdNullCount")
    println(s"window_end 为 null 的数量：$windowEndNullCount")

    val finalDF = resultDF
      .withColumn("etl_date", date_format(current_timestamp(), "yyyy-MM-dd"))
      // 使用 na.drop() 方法删除任何包含 null 的行（针对关键字段）
      .na.drop(Seq("BaseID", "window_end", "etl_date"))
      // 关键：多次过滤确保不为 null
      .filter($"BaseID".isNotNull)
      .filter($"window_end".isNotNull)
      .filter($"etl_date".isNotNull)
    // 最后验证
    val finalCount = finalDF.count()
    println(s"最终写入的数据行数：$finalCount")

    if (finalCount == 0) {
      println("警告：没有数据可以写入！请检查数据源或过滤条件。")
      spark.stop()
      return
    } else {
      // 抽样检查 - 确保这些字段真的不为 null
      println("=== 抽样检查主键字段 ===")
      finalDF.select("BaseID", "window_end", "etl_date")
        .filter($"BaseID".isNull || $"window_end".isNull || $"etl_date".isNull)
        .show(10, truncate = false)

      println("=== 正常数据样本 ===")
      finalDF.select("BaseID", "window_end", "etl_date").show(10, truncate = false)
    }

    // 强制缓存数据，避免懒执行导致的问题
    finalDF.cache()
    finalDF.count() // 触发物化

    finalDF.write
      .mode(SaveMode.Overwrite) // 按照你的要求使用 Overwrite (注意：这会覆盖分区数据)
      .format("hudi")
      // 如果有通用配置 Map，可以在这里 .options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(), "window_end")          // 预聚合字段：用于去重，取最新时间的数据
      .option(RECORDKEY_FIELD.key(), "BaseID,window_end")    // 主键：设备ID + 窗口结束时间 (联合主键)
      .option(PARTITIONPATH_FIELD.key(), "etl_date")         // 分区字段：按天分区
      .option(TBL_NAME.key(), "dwd_environment_features")    // 表名
      // 添加：关闭元数据表，避免复杂性问题
      .option("hoodie.metadata.enable", "false")
      // 添加：指定 KeyGenerator 类
      .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator")
      // 关键：添加这个配置来处理空值
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .save(s"$hdfsUrl/user/hive/warehouse/dwd_ds_hudi.db/dwd_environment_features")

    println("特征数据已写入 Hudi")

    spark.stop()
  }
}