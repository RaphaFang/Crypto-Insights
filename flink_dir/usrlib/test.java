// src/main/java/com/example/KafkaReadOnly.java
package com.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaReadOnly {

    public static void main(String[] args) throws Exception {
        // 基本參數（先寫死，之後我們再改成環境變數）
        String bootstrap = "kafka:9092";    // 你的 docker network 內的 Kafka
        String topic     = "ws_raw";        // 你的原始topic

        // 1) 建環境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2) 建 Kafka Source（value=字串）
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("flink-readonly")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())  // UTF-8 字串
                .build();

        // 3) 把資料變成 DataStream
        DataStreamSource<String> stream = env.fromSource(
                source,
                // 最小版先不設 watermark
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );

        // 4) 先看得到東西：直接印出
        stream.print();  // 之後再換成下一步的處理/視窗

        // 5) 跑起來
        env.execute("read-from-kafka-only");
    }
}
