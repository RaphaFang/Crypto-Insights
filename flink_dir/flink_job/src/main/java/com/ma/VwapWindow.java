package com.ma;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

class VwapWindow extends ProcessWindowFunction<VwapAcc, VwapResult, String, TimeWindow> {

  private final long minTrades; // 可設 0（無門檻）或 >=3
  private final BigDecimal minVolume; // 可設 BigDecimal.ZERO 或最小成交量門檻
  private final int scale; // VWAP 小數位數

  VwapWindow(long minTrades, BigDecimal minVolume, int scale) {
    this.minTrades = minTrades;
    this.minVolume = minVolume;
    this.scale = scale;
  }

  @Override
  public void process(String symbol, Context ctx, Iterable<VwapAcc> in, Collector<VwapResult> out) {
    VwapAcc acc = in.iterator().next();

    // 門檻：避免啟動初期/超稀疏時輸出噪音（不需要就設為 0）
    boolean passTrades = acc.count >= minTrades;
    boolean passVolume = acc.sumQ != null && acc.sumQ.compareTo(minVolume) >= 0;
    if (!passTrades || !passVolume)
      return;

    BigDecimal vwap = (acc.sumQ.signum() == 0)
        ? null
        : acc.sumPQ.divide(acc.sumQ, scale, RoundingMode.HALF_UP);

    long start = ctx.window().getStart();
    long end = ctx.window().getEnd();

    int secFilled = acc.secBuckets.size();
    long nowProc = ctx.currentProcessingTime(); // emit 當下的 processing time（結果輸出時間）

    VwapResult r = new VwapResult(
        symbol, start, end,
        acc.count, acc.sumQ, vwap,
        secFilled);

    r.eventTimeMin = acc.minTs;
    r.eventTimeMax = acc.maxTs;

    r.emitProcTime = nowProc;
    r.latEventToEmitMs = nowProc - acc.maxTs;
    r.latIngressFirstToEmitMs = (acc.minWmIn == Long.MAX_VALUE) ? -1L : (nowProc - acc.minWmIn);
    r.latIngressLastToEmitMs = (acc.maxWmIn == Long.MIN_VALUE) ? -1L : (nowProc - acc.maxWmIn);

    out.collect(r);
  }
}
