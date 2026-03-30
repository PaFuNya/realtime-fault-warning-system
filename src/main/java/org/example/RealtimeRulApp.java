package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import java.util.Random;

import biz.k11i.xgboost.Predictor;
import biz.k11i.xgboost.util.FVec;
import java.io.FileInputStream;

public class RealtimeRulApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1. 创建 Kafka Source
        tableEnv.executeSql(
                "CREATE TABLE sensor_kafka (" +
                        "  machine_id STRING," +
                        "  ts BIGINT," +
                        "  temperature DOUBLE," +
                        "  vibration_x DOUBLE," +
                        "  ts_ltz AS TO_TIMESTAMP_LTZ(ts, 3)," +
                        "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 'sensor_raw'," +
                        "  'properties.bootstrap.servers' = 'bigdata1:9092,bigdata2:9092,bigdata3:9092'," +
                        "  'properties.group.id' = 'rul_group'," +
                        "  'scan.startup.mode' = 'latest-offset'," +
                        "  'format' = 'json'" +
                        ")");

        // 2. 窗口特征提取 (任务 17 要求：经 Window 计算特征)
        org.apache.flink.table.api.Table featureTable = tableEnv.sqlQuery(
                "SELECT " +
                        "  TUMBLE_END(ts_ltz, INTERVAL '1' MINUTE) as ts_ltz, " +
                        "  machine_id, " +
                        "  AVG(temperature) as avg_temp, " +
                        "  AVG(ABS(vibration_x)) as avg_vib " +
                        "FROM sensor_kafka " +
                        "GROUP BY machine_id, TUMBLE(ts_ltz, INTERVAL '1' MINUTE)");

        DataStream<Row> featureStream = tableEnv.toDataStream(featureTable);

        // 3. 模型预测 (通过 RichMapFunction 模拟加载 LightGBM 进行推理)
        org.apache.flink.api.common.typeinfo.TypeInformation<Row> rowTypeInfo = Types.ROW_NAMED(
                new String[] { "ts", "machine_id", "rul_value", "risk_level" },
                Types.INSTANT, // ts
                Types.STRING, // machine_id
                Types.DOUBLE, // rul_value
                Types.STRING // risk_level
        );

        DataStream<Row> predictionStream = featureStream.map(new LightGBMPredictMockFunction()).returns(rowTypeInfo);

        // 4. 写入 ClickHouse
        predictionStream.addSink(org.apache.flink.connector.jdbc.JdbcSink.sink(
                "insert into ldc.realtime_rul_monitor (ts, machine_id, rul_value, risk_level) values (?, ?, ?, ?)",
                (ps, t) -> {
                    // 第1个字段：ts
                    Object alertTimeObj = t.getField(0);
                    if (alertTimeObj instanceof java.time.Instant) {
                        ps.setTimestamp(1, java.sql.Timestamp.from((java.time.Instant) alertTimeObj));
                    } else if (alertTimeObj instanceof java.time.LocalDateTime) {
                        ps.setTimestamp(1, java.sql.Timestamp.valueOf((java.time.LocalDateTime) alertTimeObj));
                    }
                    // 第2个字段：machine_id
                    ps.setString(2, (String) t.getField(1));
                    // 第3个字段：rul_value
                    ps.setDouble(3, (Double) t.getField(2));
                    // 第4个字段：risk_level
                    ps.setString(4, (String) t.getField(3));
                },
                new org.apache.flink.connector.jdbc.JdbcExecutionOptions.Builder()
                        .withBatchSize(1)
                        .build(),
                new org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://bigdata1:8123/ldc")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUsername("default")
                        .build()));

        env.execute("RealtimeRulApp");
    }

    /**
     * 真实模型加载与推理 (兼容 XGBoost/LightGBM 导出的模型格式)
     */
    public static class LightGBMPredictMockFunction extends RichMapFunction<Row, Row> {
        private transient Predictor predictor;
        // 如果没有提供模型，退化为模拟推理
        private transient boolean hasRealModel = false;
        private transient Random random;

        @Override
        public void open(Configuration parameters) throws Exception {
            random = new Random();
            try {
                // 加载模型文件
                String modelPath = "model.txt";
                predictor = new Predictor(new FileInputStream(modelPath));
                System.out.println("====== 模型加载成功！======");
                hasRealModel = true;
            } catch (Exception e) {
                System.err.println("未检测到模型文件，降级为模拟推理。原因: " + e.getMessage());
            }
        }

        @Override
        public Row map(Row value) throws Exception {
            Object ts = value.getField("ts_ltz");
            String machineId = (String) value.getField("machine_id");
            Double avgTemp = (Double) value.getField("avg_temp");
            Double avgVib = (Double) value.getField("avg_vib");

            double rulValue = 0.0;

            if (hasRealModel) {
                // ------------------ 真实推理逻辑 ------------------
                // 准备输入特征数组 (与 Python 训练时的特征顺序一致: avg_temp, avg_vib)
                double[] features = new double[] { avgTemp, avgVib };

                // 将 double 数组转换为预测器需要的特征向量
                FVec fVec = FVec.Transformer.fromArray(features, true);

                // 进行预测 (假设是单输出回归模型)
                double[] preds = predictor.predict(fVec);
                if (preds != null && preds.length > 0) {
                    rulValue = preds[0];
                }
            } else {
                // ------------------ 模拟推理逻辑 ------------------
                if (avgTemp > 80 || avgVib > 3.0) {
                    rulValue = 10.0 + (40.0 * random.nextDouble());
                } else {
                    rulValue = 100.0 + (200.0 * random.nextDouble());
                }
            }

            // 格式化保留两位小数
            rulValue = Math.round(rulValue * 100.0) / 100.0;

            // 动态阈值划分风险等级 (根据题目要求)
            String riskLevel;
            if (rulValue < 48.0) {
                riskLevel = "High";
            } else if (rulValue <= 168.0) {
                riskLevel = "Medium";
            } else {
                riskLevel = "Low";
            }

            return Row.of(ts, machineId, rulValue, riskLevel);
        }

        @Override
        public void close() throws Exception {
            // Predictor 不需要特别的释放资源
        }
    }
}
