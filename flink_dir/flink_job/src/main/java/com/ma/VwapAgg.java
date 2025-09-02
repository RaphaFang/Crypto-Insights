package com.ma;

import org.apache.flink.api.common.functions.AggregateFunction;

class VwapAgg implements AggregateFunction<TradeEvent, VwapAcc, VwapAcc> {

  @Override
  public VwapAcc createAccumulator() {
    return new VwapAcc();
  }

  @Override
  public VwapAcc add(TradeEvent e, VwapAcc acc) {
    if (e.price != null && e.quantity != null) {
      acc.sumPQ = acc.sumPQ.add(e.price.multiply(e.quantity));
      acc.sumQ = acc.sumQ.add(e.quantity);
      acc.count++;
      long ts = (e.tradeTime > 0 ? e.tradeTime : e.eventTime);
      acc.minTs = Math.min(acc.minTs, ts);
      acc.maxTs = Math.max(acc.maxTs, ts);
      acc.secBuckets.add(ts / 1000L); // 這邊設定是統子大小，最多 10 個
    }
    return acc;
  }

  @Override
  public VwapAcc getResult(VwapAcc acc) {
    return acc;
  }

  @Override
  public VwapAcc merge(VwapAcc a, VwapAcc b) {
    VwapAcc m = new VwapAcc();
    m.sumPQ = a.sumPQ.add(b.sumPQ);
    m.sumQ = a.sumQ.add(b.sumQ);
    m.count = a.count + b.count;
    m.minTs = Math.min(a.minTs, b.minTs);
    m.maxTs = Math.max(a.maxTs, b.maxTs);
    m.secBuckets.addAll(a.secBuckets);
    m.secBuckets.addAll(b.secBuckets);
    return m;
  }
}
