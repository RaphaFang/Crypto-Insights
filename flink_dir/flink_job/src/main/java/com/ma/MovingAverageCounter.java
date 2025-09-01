package com.ma;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 
// mvn -U -DskipTests clean package
// cp target/flink-job-1.0.0.jar ../usrlib
// 

public class MovingAverageCounter {

        public static void main(String[] args) throws Exception {
                String bootstrap = "kafka:9092";
                String topic = "btc_raw";

                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

                // 0. deserialization by my own schema
                KafkaSource<TradeEvent> source = KafkaSource.<TradeEvent>builder()
                                .setBootstrapServers(bootstrap)
                                .setTopics(topic)
                                .setGroupId("flink-readonly")
                                .setStartingOffsets(OffsetsInitializer.latest())
                                .setValueOnlyDeserializer(new TradeEventDeserializationSchema())
                                .build();

                DataStream<TradeEvent> trades = env
                                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-trade-source");

                trades
                                .keyBy(t -> t.symbol)
                                .print();

                env.execute("normalize-trades");
        }
}
