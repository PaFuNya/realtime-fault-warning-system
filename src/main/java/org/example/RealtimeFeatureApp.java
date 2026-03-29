package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RealtimeFeatureApp {
        public static void main(String[] args) throws Exception {
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

                // 1. 接入 Kafka 传感器
                tableEnv.executeSql(
                                "CREATE TABLE sensor_kafka (" +
                                                "  machine_id STRING," +
                                                "  ts BIGINT," +
                                                "  temperature DOUBLE," +
                                                "  `current` DOUBLE," +
                                                "  vibration_x DOUBLE," +
                                                "  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3)," +
                                                "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND" +
                                                ") WITH (" +
                                                "  'connector' = 'kafka'," +
                                                "  'topic' = 'sensor_raw'," +
                                                "  'properties.bootstrap.servers' = 'bigdata1:9092,bigdata2:9092,bigdata3:9092',"
                                                +
                                                "  'properties.group.id' = 'sensor_feature_group'," +
                                                "  'scan.startup.mode' = 'latest-offset'," +
                                                "  'format' = 'json'" +
                                                ")");

                // 2. 计算特征：在这里直接用 ROUND 精简数字
                tableEnv.executeSql(
                                "CREATE VIEW feature_view AS " +
                                                "SELECT " +
                                                "  machine_id, " +
                                                "  TUMBLE_START(ts_ltz, INTERVAL '1' MINUTE) as window_start, " +
                                                "  TUMBLE_END(ts_ltz, INTERVAL '1' MINUTE) as window_end, " +
                                                "  ROUND(MAX(temperature) - MIN(temperature), 2) as temp_slope, " + // 保留2位
                                                "  ROUND(MAX(`current`) - MIN(`current`), 2) as current_fluctuation, " + // 保留2位
                                                "  ROUND(AVG(ABS(vibration_x)), 3) as vibration_rms, " + // 振动建议保留3位
                                                "  CASE WHEN AVG(temperature) > 80 THEN 'WARNING' ELSE 'STABLE' END as status "
                                                +
                                                "FROM sensor_kafka " +
                                                "GROUP BY machine_id, TUMBLE(ts_ltz, INTERVAL '1' MINUTE)");

                // 3. 转换为流并写入 ClickHouse
                org.apache.flink.table.api.Table table = tableEnv.from("feature_view");
                org.apache.flink.streaming.api.datastream.DataStream<org.apache.flink.types.Row> ds = tableEnv
                                .toDataStream(table);

                ds.addSink(org.apache.flink.connector.jdbc.JdbcSink.sink(
                                "insert into ldc.device_realtime_status (machine_id, window_start, window_end, temp_slope, current_fluctuation, vibration_rms, status) values (?, ?, ?, ?, ?, ?, ?)",
                                (ps, t) -> {
                                        ps.setString(1, (String) t.getField("machine_id"));
                                        ps.setTimestamp(2, java.sql.Timestamp
                                                        .valueOf((java.time.LocalDateTime) t.getField("window_start")));
                                        ps.setTimestamp(3, java.sql.Timestamp
                                                        .valueOf((java.time.LocalDateTime) t.getField("window_end")));
                                        // 这里要特别注意：ROUND 之后 Flink 可能返回 Double，也可能返回 BigDecimal
                                        // 我们统一转成 String 再转 Double 最安全，防止类型转换报错
                                        ps.setDouble(4, Double.parseDouble(t.getField("temp_slope").toString()));
                                        ps.setDouble(5, Double
                                                        .parseDouble(t.getField("current_fluctuation").toString()));
                                        ps.setDouble(6, Double.parseDouble(t.getField("vibration_rms").toString()));
                                        ps.setString(7, (String) t.getField("status"));
                                },
                                new org.apache.flink.connector.jdbc.JdbcExecutionOptions.Builder()
                                                .withBatchSize(1)
                                                .build(),
                                new org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                                .withUrl("jdbc:clickhouse://bigdata1:8123/ldc")
                                                .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                                                .withUsername("default")
                                                .build()));

                env.execute("RealtimeFeatureApp-CleanData");
        }
}