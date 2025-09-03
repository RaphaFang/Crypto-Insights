package com.ma;

import java.math.BigDecimal;
import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
// import org.apache.flink.streaming.api.windowing.time.Time;

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

    // ! 1. Watermark, with maximum 2 sec delay
    WatermarkStrategy<TradeEvent> wm = WatermarkStrategy
        .<TradeEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(
            (SerializableTimestampAssigner<TradeEvent>) (e, ts) -> (e.tradeTime > 0 ? e.tradeTime : e.eventTime))
        .withIdleness(Duration.ofSeconds(30));

    DataStream<TradeEvent> trades = env.fromSource(source, wm, "kafka-trade-source")
        .process(new StampWmIn()) // ← 取代原本的 .map(...)
        .name("stamp-wm_in");
    // TradeEvent{eventType='trade', eventTime=1756849656824, symbol='BTCUSDT',
    // tradeId=5210609518, price=111255.62000000, quantity=0.01204000,
    // tradeTime=1756849656823, isBuyerMarketMaker=false, ignore=true,
    // wmIn=1756849656939}

    // ! 2. 10s 視窗、1s 滑動 → VWAP
    Duration SIZE = Duration.ofSeconds(10);
    Duration SLIDE = Duration.ofSeconds(1);
    final long MIN_TRADES = 0L;
    final BigDecimal MIN_VOLUME = BigDecimal.ZERO;
    final int SCALE = 8;

    DataStream<VwapResult> vwap10s1s = trades
        .keyBy(t -> t.symbol)
        .window(SlidingEventTimeWindows.of(SIZE, SLIDE))
        .allowedLateness(Duration.ofSeconds(1))
        .aggregate(new VwapAgg(), new VwapWindow(MIN_TRADES, MIN_VOLUME, SCALE))
        .name("vwap_10s_slide_1s");
    // VwapResult{symbol='BTCUSDT', windowStart=1756847091000,
    // windowEnd=1756847101000, tradeCount=22, qtySum=0.10903000,
    // vwap=111371.48183711, secFilled=9, expectedSeconds=10}

    vwap10s1s.print();

    env.execute("normalize-trades + vwap_10s_1s");
  }
}
