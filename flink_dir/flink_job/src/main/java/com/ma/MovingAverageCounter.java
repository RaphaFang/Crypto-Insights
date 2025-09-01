package com.ma;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// 
// cd flink_dir/flink_job
// mvn -U -DskipTests clean package
// cp target/flink-job-1.0.0.jar ../usrlib
// 

public class MovingAverageCounter {

  public static void main(String[] args) throws Exception {
    String bootstrap = "kafka:9092";
    String topic = "btc_raw";

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // ! 0. deserialization by my own schema
    KafkaSource<TradeEvent> source = KafkaSource.<TradeEvent>builder()
        .setBootstrapServers(bootstrap)
        .setTopics(topic)
        .setGroupId("flink-readonly")
        .setStartingOffsets(OffsetsInitializer.latest())
        .setValueOnlyDeserializer(new TradeEventDeserializationSchema())
        .build();

    // DataStream<TradeEvent> trades = env
    // .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-trade-source");

    // ! 1. Watermark, with maximum 2 sec delay
    WatermarkStrategy<TradeEvent> wm = WatermarkStrategy
        .<TradeEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(
            (SerializableTimestampAssigner<TradeEvent>) (e, ts) -> (e.tradeTime > 0 ? e.tradeTime : e.eventTime))
        .withIdleness(Duration.ofSeconds(30));

    // 2) 用 ProcessFunction 壓入當下處理時間到 wmIn
    DataStream<TradeEvent> trades = env.fromSource(source, wm, "kafka-trade-source")
        .process(new StampWmIn()) // ← 取代原本的 .map(...)
        .name("stamp-wm_in");

    trades
        .keyBy(t -> t.symbol)
        .print();

    env.execute("normalize-trades");
  }
}
