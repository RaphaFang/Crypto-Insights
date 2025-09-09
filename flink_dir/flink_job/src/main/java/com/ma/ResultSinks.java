package com.ma;

import java.util.Properties;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;

final class ResultSinks {
  private ResultSinks() {
  }

  static KafkaSink<VwapResult> buildHistSink(String bootstrap, String topic) {
    Properties props = new Properties();
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
    props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32_768);

    return KafkaSink.<VwapResult>builder()
        .setBootstrapServers(bootstrap)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.<VwapResult>builder()
                .setTopic(topic) // e.g. "agg.vwap"
                .setKeySerializationSchema(new ResultKeyHist())
                .setValueSerializationSchema(new ResultJsonSeri())
                .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .setKafkaProducerConfig(props)
        .build();
  }
}