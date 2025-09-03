package com.ma;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.SerializationSchema;

class ResultKeyHist implements SerializationSchema<VwapResult> {
  @Override
  public byte[] serialize(VwapResult r) {
    String key = r.symbol + "|" + r.ma + "|" + r.windowEnd;
    return key.getBytes(StandardCharsets.UTF_8);
  }
}
