package com.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 
// mvn -U -DskipTests clean package
// cp target/flink-job-1.0.0.jar ../flink_dir/usrlib
// 

public class MovingAverageCounter {

    public static void main(String[] args) throws Exception {
        String bootstrap = "kafka:9092";
        String topic = "btc_raw";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("flink-readonly")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> stream = env.fromSource(
                source,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "kafka-source");

        stream.print();

        env.execute("read-from-kafka-only");
    }
}
