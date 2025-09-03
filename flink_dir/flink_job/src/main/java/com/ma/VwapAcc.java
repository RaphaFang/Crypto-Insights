package com.ma;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.HashSet;

class VwapAcc implements Serializable {
  BigDecimal sumPQ = BigDecimal.ZERO; // Σ(p*q)
  BigDecimal sumQ = BigDecimal.ZERO; // Σ(q)
  long count = 0L; // 筆數
  long minTs = Long.MAX_VALUE; // 視窗內最小事件時間（ms）
  long maxTs = Long.MIN_VALUE; // 視窗內最大事件時間（ms）
  long minWmIn = Long.MAX_VALUE; // 此窗內最早的 ingress processing time
  long maxWmIn = Long.MIN_VALUE; // 此窗內最晚的 ingress processing time
  // 這 10 秒內有交易的「秒」集合（用 tradeTime/eventTime → 秒）
  HashSet<Long> secBuckets = new HashSet<>(12);
}
