package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;

public class RealtimeDeviationApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 创建 Kafka Source
        tableEnv.executeSql(
                "CREATE TABLE sensor_kafka (" +
                        "  machine_id STRING," +
                        "  ts BIGINT," +
                        "  `current` DOUBLE," + // 改为电流
                        "  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3)," +
                        "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND" + // 需要窗口计算，必须加 Watermark
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 'sensor_raw'," +
                        "  'properties.bootstrap.servers' = 'bigdata1:9092,bigdata2:9092,bigdata3:9092'," +
                        "  'properties.group.id' = 'deviation_group'," +
                        "  'scan.startup.mode' = 'latest-offset'," +
                        "  'format' = 'json'" +
                        ")");

        // 2. 将表转为流
        org.apache.flink.table.api.Table table = tableEnv
                .sqlQuery("SELECT machine_id, ts_ltz, `current` FROM sensor_kafka");
        DataStream<Row> kafkaStream = tableEnv.toDataStream(table);

        // 3. 使用 RichMapFunction 关联 Redis 维表，计算每条数据的瞬时偏差
        // 因为 Flink 无法自动推断泛型 Row 的字段名和类型，所以我们需要显式提供 TypeInformation
        org.apache.flink.api.common.typeinfo.TypeInformation<Row> rowTypeInfo = org.apache.flink.api.common.typeinfo.Types
                .ROW_NAMED(
                        new String[] { "ts_ltz", "machine_id", "actual_current", "standard_current",
                                "current_deviation" },
                        org.apache.flink.api.common.typeinfo.Types.INSTANT, // ts_ltz (注意在 Flink SQL 中对应 TIMESTAMP_LTZ)
                        org.apache.flink.api.common.typeinfo.Types.STRING, // machine_id
                        org.apache.flink.api.common.typeinfo.Types.DOUBLE, // actual_current
                        org.apache.flink.api.common.typeinfo.Types.DOUBLE, // standard_current
                        org.apache.flink.api.common.typeinfo.Types.DOUBLE // current_deviation
                );

        DataStream<Row> mappedStream = kafkaStream.map(new RedisLookupFunction()).returns(rowTypeInfo);

        // 将关联好标准值的数据重新注册为虚拟表，方便后续做 30 秒窗口判断
        tableEnv.createTemporaryView("mapped_stream", mappedStream,
                org.apache.flink.table.api.Schema.newBuilder()
                        .column("ts_ltz", org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ(3))
                        .column("machine_id", org.apache.flink.table.api.DataTypes.STRING())
                        .column("actual_current", org.apache.flink.table.api.DataTypes.DOUBLE())
                        .column("standard_current", org.apache.flink.table.api.DataTypes.DOUBLE())
                        .column("current_deviation", org.apache.flink.table.api.DataTypes.DOUBLE())
                        .watermark("ts_ltz", "ts_ltz - INTERVAL '5' SECOND") // 继承水印
                        .build());

        // 4. 核心考点：持续偏离 30 秒即记录
        // 逻辑：在 30 秒的滚动窗口内，如果该机器的"最小绝对偏差"仍然大于 10% (0.1)，说明这 30 秒内它一直在偏离
        org.apache.flink.table.api.Table windowedAlertTable = tableEnv.sqlQuery(
                "SELECT " +
                        "  TUMBLE_END(ts_ltz, INTERVAL '30' SECOND) as ts, " +
                        "  machine_id, " +
                        "  AVG(actual_current) as actual_current, " +
                        "  MAX(standard_current) as standard_current, " +
                        "  MAX(ABS(current_deviation)) as max_deviation " + // 验证要求：查询偏差最大的记录
                        "FROM mapped_stream " +
                        "GROUP BY machine_id, TUMBLE(ts_ltz, INTERVAL '30' SECOND) " +
                        "HAVING MIN(ABS(current_deviation)) > 10.0"); // 假设偏离度超过 10% 算偏离

        DataStream<Row> deviationStream = tableEnv.toDataStream(windowedAlertTable);

        // 4. 使用 JdbcSink 写入 ClickHouse (类似之前的任务)
        deviationStream.addSink(org.apache.flink.connector.jdbc.JdbcSink.sink(
                "insert into ldc.realtime_deviation_monitor (ts, machine_id, actual_temp, standard_temp, temp_deviation) values (?, ?, ?, ?, ?)",
                (ps, t) -> {
                    // 第1个字段：ts (处理 Instant 或 LocalDateTime)
                    Object alertTimeObj = t.getField(0);
                    if (alertTimeObj instanceof java.time.Instant) {
                        ps.setTimestamp(1, java.sql.Timestamp.from((java.time.Instant) alertTimeObj));
                    } else if (alertTimeObj instanceof java.time.LocalDateTime) {
                        ps.setTimestamp(1, java.sql.Timestamp.valueOf((java.time.LocalDateTime) alertTimeObj));
                    }

                    // 第2个字段：machine_id
                    ps.setString(2, (String) t.getField(1));
                    // 第3个字段：actual_temp
                    ps.setDouble(3, (Double) t.getField(2));
                    // 第4个字段：standard_temp
                    ps.setDouble(4, (Double) t.getField(3));
                    // 第5个字段：temp_deviation
                    ps.setDouble(5, (Double) t.getField(4));
                },
                new org.apache.flink.connector.jdbc.JdbcExecutionOptions.Builder()
                        .withBatchSize(1)
                        .build(),
                new org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://bigdata1:8123/ldc")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername("default")
                        .build()));

        env.execute("RealtimeDeviationApp");
    }

    // 这是一个内部类，用来查询 Redis 并计算偏离度
    public static class RedisLookupFunction extends RichMapFunction<Row, Row> {
        private transient Jedis jedis;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 在这里初始化 Redis 连接
            jedis = new Jedis("bigdata1", 6379); // 替换成你实际的 Redis IP
            // 如果有密码：jedis.auth("your_password");
        }

        @Override
        public Row map(Row value) throws Exception {
            // 1. 从 Kafka 的 Row 中提取数据
            String machineId = (String) value.getField("machine_id");
            Double actualCurrent = (Double) value.getField("current");
            Object ts = value.getField("ts_ltz");

            // 2. 查 Redis，由于现在是查电流，假设 Redis 里的 Key 是 "std_current:Machine_001"
            String stdCurrentStr = jedis.get("std_current:" + machineId);

            // 3. 计算偏离度并返回新的 Row
            Double stdCurrent = 10.0; // 默认标准电流防空指针
            if (stdCurrentStr != null) {
                stdCurrent = Double.parseDouble(stdCurrentStr);
            }

            // 偏离度 = (实际 - 标准) / 标准 * 100
            Double deviation = (actualCurrent - stdCurrent) / stdCurrent * 100;

            // 返回一个新的 Row 给 mapped_stream: ts_ltz, machine_id, actual_current,
            // standard_current, current_deviation
            return Row.of(ts, machineId, actualCurrent, stdCurrent, deviation);
        }

        @Override
        public void close() throws Exception {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}