package com.ma;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

final class ResultSinks {
  private ResultSinks() {
  }

  static KafkaSink<VwapResult> buildHistSink(String bootstrap, String topic) {
    return KafkaSink.<VwapResult>builder()
        .setBootstrapServers(bootstrap)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.<VwapResult>builder()
                .setTopic(topic) // 例如 "agg.vwap"
                .setKeySerializationSchema(new ResultKeyHist())
                .setValueSerializationSchema(new ResultJsonSeri())
                .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE) // ← 正確的方法名
        .build();
  }
}
