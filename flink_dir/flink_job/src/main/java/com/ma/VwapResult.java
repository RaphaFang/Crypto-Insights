package com.ma;

import java.io.Serializable;
import java.math.BigDecimal;

public class VwapResult implements Serializable {
  public String symbol;
  public long windowStart;
  public long windowEnd;
  public long tradeCount; // 視窗內成交筆數
  public BigDecimal qtySum; // Σ(q)
  public BigDecimal vwap; // Σ(p*q)/Σ(q)
  public int secFilled; // 這個 10 秒裡有交易的「秒」數
  public int expectedSeconds; // 理論秒數（size/1s），10 秒窗就是 10

  public VwapResult() {
  }

  public VwapResult(String symbol, long windowStart, long windowEnd,
      long tradeCount, BigDecimal qtySum, BigDecimal vwap,
      int secFilled, int expectedSeconds) {
    this.symbol = symbol;
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
    this.tradeCount = tradeCount;
    this.qtySum = qtySum;
    this.vwap = vwap;
    this.secFilled = secFilled;
    this.expectedSeconds = expectedSeconds;
  }

  @Override
  public String toString() {
    return "VwapResult{" +
        "symbol='" + symbol + '\'' +
        ", windowStart=" + windowStart +
        ", windowEnd=" + windowEnd +
        ", tradeCount=" + tradeCount +
        ", qtySum=" + qtySum +
        ", vwap=" + vwap +
        ", secFilled=" + secFilled +
        ", expectedSeconds=" + expectedSeconds +
        '}';
  }
}
