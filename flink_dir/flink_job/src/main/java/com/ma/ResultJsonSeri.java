package com.ma;

import org.apache.flink.api.common.serialization.SerializationSchema;

import com.fasterxml.jackson.databind.ObjectMapper;

class ResultJsonSeri implements SerializationSchema<VwapResult> {
  private static final ObjectMapper M = new ObjectMapper();

  @Override
  public byte[] serialize(VwapResult r) {
    try {
      return M.writeValueAsBytes(r);
    } catch (Exception e) {
      throw new RuntimeException("serialize VwapResult failed", e);
    }
  }
}
