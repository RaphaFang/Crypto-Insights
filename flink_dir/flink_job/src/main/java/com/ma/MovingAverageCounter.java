package com.ma;

import java.math.BigDecimal;
import java.time.Duration;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

// 
// cd flink_dir/flink_job
// mvn -U -DskipTests clean package
// cp target/flink-job-1.0.0.jar ../usrlib
// 
// cd /Users/fangsiyu/Desktop/Crypto_K_F
// 總覺得要設計檢查，晚到被丟掉的資料，secFilled 常常不是滿的
// 還要記得跳轉回原先的 cd ，因為後面 git
public class MovingAverageCounter {

  public static void main(String[] args) throws Exception {
    String bootstrap = "kafka:9092";
    String topic = "btc_raw";

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(10_000, CheckpointingMode.AT_LEAST_ONCE);

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
        .process(new StampWmIn())
        .name("stamp-wm_in");

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

    // ! 3. Back to kafka, at topic agg.vwap
    String outTopic = "agg.vwap"; // 這邊是 topic name
    vwap10s1s.sinkTo(ResultSinks.buildHistSink(bootstrap, outTopic))
        .name("sink-agg.vwap");

    // vwap10s1s.print();
    env.execute("normalize-trades + vwap_10s_1s → kafka");

    /*
     * "symbol='" + symbol + '\'' +
     * ", ma=" + ma +
     * 
     * ", windowStart=" + windowStart +
     * ", windowEnd=" + windowEnd +
     * ", rawDataCount=" + rawDataCount +
     * 
     * ", qtySum=" + qtySum +
     * ", vwap=" + vwap +
     * 
     * ", secFilled=" // ! 用事件時間（tradeTime；沒有就 eventTime）把每筆交易按「秒」分桶
     * ", eventTimeMin=" + eventTimeMin + // 最小的事件時間
     * ", eventTimeMax=" + eventTimeMax +
     * 
     * ", emitTime=" + emitTime + // 出去的時間
     * ", eventToEmitLatencyMs=" //! 發出時間，減上最後資料事件時間。基於 事件時間 的延遲計算
     * ", firstIngestToEmitLatencyMs=" // 發出時間，減上wm。
     * ", lastIngestToEmitLatencyMs=" //! 發出時間，減上wm。基於 wm 時間的延遲計算
     * 發出時間，減上最後事件時間。基於收到資料的延遲計算這兩者的差異不用過分解讀成在kafka卡10s，有很多因素
     * '}';
     */
  }
}
