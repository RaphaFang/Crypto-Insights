package com.ma;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TradeEventDeserializationSchema implements DeserializationSchema<TradeEvent> {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public TradeEvent deserialize(byte[] message) throws IOException {
    JsonNode n = MAPPER.readTree(message);

    TradeEvent e = new TradeEvent();
    e.eventType = text(n, "e");
    e.eventTime = longVal(n, "E");
    e.symbol = text(n, "s");
    e.tradeId = longVal(n, "t");
    e.price = decimal(n, "p");
    e.quantity = decimal(n, "q");
    e.tradeTime = longVal(n, "T");
    e.isBuyerMarketMaker = boolVal(n, "m");
    e.ignore = boolVal(n, "M");
    return e;
  }

  private static String text(JsonNode n, String k) {
    JsonNode v = n.get(k);
    return v == null || v.isNull() ? null : v.asText();
  }

  private static long longVal(JsonNode n, String k) {
    JsonNode v = n.get(k);
    return (v == null || v.isNull()) ? 0L : v.asLong();
  }

  private static boolean boolVal(JsonNode n, String k) {
    JsonNode v = n.get(k);
    return v != null && v.asBoolean(false);
  }

  private static BigDecimal decimal(JsonNode n, String k) {
    JsonNode v = n.get(k);
    if (v == null || v.isNull())
      return null;
    // Binance 價格/數量是字串，小數精度用 BigDecimal
    return new BigDecimal(v.asText());
  }

  @Override
  public boolean isEndOfStream(TradeEvent nextElement) {
    return false;
  }

  @Override
  public TypeInformation<TradeEvent> getProducedType() {
    return TypeInformation.of(TradeEvent.class);
  }
}
