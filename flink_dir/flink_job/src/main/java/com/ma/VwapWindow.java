package com.ma;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;

class VwapWindow extends ProcessWindowFunction<VwapAcc, VwapResult, String, TimeWindow> {

  private final long minTrades; // 例如 0 或 3
  private final BigDecimal minVolume; // 例如 BigDecimal.ZERO 或 0.001
  private final int scale; // VWAP 小數位數

  VwapWindow(long minTrades, BigDecimal minVolume, int scale) {
    this.minTrades = minTrades;
    this.minVolume = minVolume;
    this.scale = scale;
  }

  @Override
  public void process(String symbol, Context ctx, Iterable<VwapAcc> in, Collector<VwapResult> out) {
    VwapAcc acc = in.iterator().next();

    boolean passTrades = acc.count >= minTrades;
    boolean passVolume = (acc.sumQ != null && acc.sumQ.compareTo(minVolume) >= 0);
    if (!passTrades || !passVolume)
      return;

    BigDecimal vwap = (acc.sumQ.signum() == 0)
        ? null
        : acc.sumPQ.divide(acc.sumQ, scale, RoundingMode.HALF_UP);

    long start = ctx.window().getStart();
    long end = ctx.window().getEnd();
    int secFilled = acc.secBuckets.size();
    int expectedSeconds = (int) ((end - start) / 1000L);

    out.collect(new VwapResult(
        symbol, start, end,
        acc.count, acc.sumQ, vwap,
        secFilled, expectedSeconds));
  }
}
