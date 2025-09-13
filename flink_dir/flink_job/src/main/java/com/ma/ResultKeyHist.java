package com.ma;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.SerializationSchema;

class ResultKeyHist implements SerializationSchema<VwapResult> {
  @Override
  public byte[] serialize(VwapResult r) {
    String key = r.symbol + "|" + r.ma;
    return key.getBytes(StandardCharsets.UTF_8);
  }
}
// + "|" + r.windowEnd
// 我先前在這邊加上這段，會導致美筆資料都不同key，導致大家分區都變得隨機了
// Sum up, 如果是同樣的 key ，一定會在同一區。但是不同key，也可能到你那區或是不同區。