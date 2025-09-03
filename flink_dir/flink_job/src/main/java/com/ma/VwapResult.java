package com.ma;

import java.io.Serializable;
import java.math.BigDecimal;

public class VwapResult implements Serializable {
  public String symbol;

  public long windowStart; // 視窗左界（事件時間，ms）
  public long windowEnd; // 視窗右界（事件時間，ms）
  public long tradeCount; // 視窗內訊息筆數（每筆都計）

  public BigDecimal qtySum; // Σ(quantity)
  public BigDecimal vwap; // Σ(price*quantity)/Σ(quantity)

  public int secFilled; // 這個 10 秒窗內實際有成交的「秒」數
  public int expectedSeconds; // (windowEnd - windowStart)/1000，通常 10

  public long eventTimeMin; // 視窗內最小事件時間（ms）

  public long emitProcTime; // 視窗輸出當下的 processing time
  public long latEventToEmitMs; // emitProcTime - maxTs
  public long latIngressFirstToEmitMs; // emitProcTime - minWmIn
  public long latIngressLastToEmitMs; // emitProcTime - maxWmIn

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
        ", eventTimeMin=" + eventTimeMin +
        ", emitProcTime=" + emitProcTime +
        ", latEventToEmitMs=" + latEventToEmitMs +
        ", latIngressFirstToEmitMs=" + latIngressFirstToEmitMs +
        ", latIngressLastToEmitMs=" + latIngressLastToEmitMs +
        '}';
  }
}
