package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RealtimeHudiApp {
    public static void main(String[] args) throws Exception {
        // 避坑：配置运行时的 Hadoop 用户名，防止 HDFS 写入时报权限不足
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ⚠️ 维稳神药 1：强制单并发，防止服务器 Slot 资源不足导致分配死锁
        env.setParallelism(1);

        // ⚠️ 维稳神药 2：配置 Checkpoint 存储到 HDFS (彻底抛弃 VPN IP，使用内网主机名 bigdata1)
        env.enableCheckpointing(10000); // 每 10 秒触发一次
        env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata1:9000/user/flink/checkpoints");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // ======================= 1. 传感器流处理 =======================
        tableEnv.executeSql(
                "CREATE TABLE sensor_kafka (" +
                        "  machine_id STRING," +
                        "  ts BIGINT," +
                        "  temperature DOUBLE," +
                        "  vibration_x DOUBLE," +
                        "  vibration_y DOUBLE," +
                        "  vibration_z DOUBLE," +
                        "  `current` DOUBLE," +
                        "  current_val AS `current`," +
                        "  noise DOUBLE," +
                        "  speed INT," +
                        "  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3)," +
                        "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 'sensor_raw'," +
                        "  'properties.bootstrap.servers' = 'bigdata1:9092,bigdata2:9092,bigdata3:9092'," + // 💡 换成内网主机名
                        "  'properties.group.id' = 'hudi_sensor_group'," +
                        "  'scan.startup.mode' = 'latest-offset'," +
                        "  'format' = 'json'" +
                        ")"
        );

        tableEnv.executeSql(
                "CREATE VIEW cleaned_sensor_view AS " +
                        "SELECT " +
                        "  machine_id, ts, temperature, vibration_x, vibration_y, vibration_z, current_val, noise, speed " +
                        "FROM sensor_kafka " +
                        "WHERE temperature BETWEEN -50 AND 150 " +
                        "  AND current_val >= 0 " +
                        "  AND speed >= 0"
        );

        tableEnv.executeSql(
                "CREATE TABLE sensor_hudi_sink (" +
                        "  machine_id STRING," +
                        "  ts BIGINT," +
                        "  temperature DOUBLE," +
                        "  vibration_x DOUBLE," +
                        "  vibration_y DOUBLE," +
                        "  vibration_z DOUBLE," +
                        "  current_val DOUBLE," +
                        "  noise DOUBLE," +
                        "  speed INT," +
                        "  PRIMARY KEY (machine_id, ts) NOT ENFORCED" +
                        ") WITH (" +
                        "  'connector' = 'hudi'," +
                        "  'path' = 'hdfs://bigdata1:9000/user/hudi/dwd_hudi/sensor_detail_realtime'," + // 💡 换成内网主机名
                        "  'table.type' = 'MERGE_ON_READ'," +
                        "  'write.operation' = 'upsert'," +
                        "  'compaction.async.enabled' = 'true'," +
                        "  'precombine.field' = 'ts'" +
                        ")"
        );

        // ======================= 2. 日志流处理 =======================
        tableEnv.executeSql(
                "CREATE TABLE log_kafka (" +
                        "  machine_id STRING," +
                        "  ts BIGINT," +
                        "  error_code STRING," +
                        "  error_msg STRING," +
                        "  stack_trace STRING" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 'log_raw'," +
                        "  'properties.bootstrap.servers' = 'bigdata1:9092,bigdata2:9092,bigdata3:9092'," + // 💡 换成内网主机名
                        "  'properties.group.id' = 'hudi_log_group'," +
                        "  'scan.startup.mode' = 'latest-offset'," +
                        "  'format' = 'json'" +
                        ")"
        );

        tableEnv.executeSql(
                "CREATE TABLE log_hudi_sink (" +
                        "  machine_id STRING," +
                        "  ts BIGINT," +
                        "  error_code STRING," +
                        "  error_msg STRING," +
                        "  stack_trace STRING," +
                        "  urgency_level STRING," +
                        "  PRIMARY KEY (machine_id, ts) NOT ENFORCED" +
                        ") WITH (" +
                        "  'connector' = 'hudi'," +
                        "  'path' = 'hdfs://bigdata1:9000/user/hudi/dwd_hudi/device_log_realtime'," + // 💡 换成内网主机名
                        "  'table.type' = 'MERGE_ON_READ'," +
                        "  'write.operation' = 'upsert'," +
                        "  'precombine.field' = 'ts'" +
                        ")"
        );

        // ======================= 3. 绝杀优化：合并提交任务 =======================
        StatementSet stmtSet = tableEnv.createStatementSet();

        // 动作 1：添加传感器写入计划
        stmtSet.addInsertSql("INSERT INTO sensor_hudi_sink SELECT * FROM cleaned_sensor_view");

        // 动作 2：添加日志写入计划
        stmtSet.addInsertSql(
                "INSERT INTO log_hudi_sink " +
                        "SELECT " +
                        "  machine_id, ts, error_code, error_msg, stack_trace, " +
                        "  CASE " +
                        "    WHEN error_code = '999' THEN 'CRITICAL' " +
                        "    WHEN error_code IN ('500', '502') THEN 'HIGH' " +
                        "    ELSE 'LOW' " +
                        "  END AS urgency_level " +
                        "FROM log_kafka"
        );

        // 动作 3：一次性统一触发执行！
        stmtSet.execute();
    }
}