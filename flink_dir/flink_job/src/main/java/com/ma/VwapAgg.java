package com.ma;

import org.apache.flink.api.common.functions.AggregateFunction;

/** 把每筆 TradeEvent 納入 VWAP 累計與觀測統計 */
class VwapAgg implements AggregateFunction<TradeEvent, VwapAcc, VwapAcc> {

  @Override
  public VwapAcc createAccumulator() {
    return new VwapAcc();
  }

  @Override
  public VwapAcc add(TradeEvent e, VwapAcc acc) {
    // 1) 訊息筆數：每筆都計
    acc.count++;

    // 2) 事件時間追蹤
    long ts = (e.tradeTime > 0 ? e.tradeTime : e.eventTime);
    if (ts < acc.minTs)
      acc.minTs = ts;
    if (ts > acc.maxTs)
      acc.maxTs = ts;
    acc.secBuckets.add(ts / 1000L);

    // 3) ingress processing time（wmIn）追蹤
    if (e.wmIn > 0) {
      if (e.wmIn < acc.minWmIn)
        acc.minWmIn = e.wmIn;
      if (e.wmIn > acc.maxWmIn)
        acc.maxWmIn = e.wmIn;
    }

    // 4) VWAP 累加（僅在價量齊全時）
    if (e.price != null && e.quantity != null) {
      acc.sumPQ = acc.sumPQ.add(e.price.multiply(e.quantity));
      acc.sumQ = acc.sumQ.add(e.quantity);
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

    m.minWmIn = Math.min(a.minWmIn, b.minWmIn);
    m.maxWmIn = Math.max(a.maxWmIn, b.maxWmIn);

    m.secBuckets.addAll(a.secBuckets);
    m.secBuckets.addAll(b.secBuckets);
    return m;
  }
}
