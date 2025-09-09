package com.ma;

import org.apache.flink.api.common.serialization.SerializationSchema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

final class ResultJsonSeri implements SerializationSchema<VwapResult> {
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .configure(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN, true) // 這邊是處理去掉資料開頭的機制
      .setSerializationInclusion(JsonInclude.Include.NON_NULL); // 去掉 null 欄位

  @Override
  public byte[] serialize(VwapResult r) {
    try {
      return MAPPER.writeValueAsBytes(r); // UTF-8 緊湊 JSON（單行）
    } catch (Exception e) {
      throw new RuntimeException("serialize VwapResult failed", e);
    }
  }
}