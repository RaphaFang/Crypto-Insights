package com.ma;

import java.io.Serializable;
import java.math.BigDecimal;

public class VwapResult implements Serializable {
  public String symbol;
  public int ma = 10;

  public long windowStart; // 視窗左界（事件時間，ms）
  public long windowEnd; // 視窗右界（事件時間，ms）
  public long rawDataCount; // 視窗內訊息筆數（每筆都計）

  public BigDecimal qtySum; // Σ(quantity)
  public BigDecimal vwap; // Σ(price*quantity)/Σ(quantity)

  public int secFilled; // 這個 10 秒窗內實際有成交的「秒」數

  public long eventTimeMin; // 視窗內最小事件時間（ms）
  public long eventTimeMax; // 視窗內最大事件時間（ms）

  public long emitTime; // 視窗輸出當下的 processing time
  public long eventToEmitLatencyMs; // emitTime - maxTs
  public long firstIngestToEmitLatencyMs; // emitTime - minWmIn
  public long lastIngestToEmitLatencyMs; // emitTime - maxWmIn

  public VwapResult() {
  }

  public VwapResult(String symbol, long windowStart, long windowEnd,
      long rawDataCount, BigDecimal qtySum, BigDecimal vwap,
      int secFilled) {
    this.symbol = symbol;
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
    this.rawDataCount = rawDataCount;
    this.qtySum = qtySum;
    this.vwap = vwap;
    this.secFilled = secFilled;
  }

  @Override
  public String toString() {
    return "VwapResult{" +
        "symbol='" + symbol + '\'' +
        ", ma=" + ma +

        ", windowStart=" + windowStart +
        ", windowEnd=" + windowEnd +
        ", rawDataCount=" + rawDataCount +

        ", qtySum=" + qtySum +
        ", vwap=" + vwap +
        ", secFilled=" + secFilled +

        ", eventTimeMin=" + eventTimeMin +
        ", eventTimeMax=" + eventTimeMax +

        ", emitTime=" + emitTime +
        ", eventToEmitLatencyMs=" + eventToEmitLatencyMs +
        ", firstIngestToEmitLatencyMs=" + firstIngestToEmitLatencyMs +
        ", lastIngestToEmitLatencyMs=" + lastIngestToEmitLatencyMs +
        '}';
  }
}
