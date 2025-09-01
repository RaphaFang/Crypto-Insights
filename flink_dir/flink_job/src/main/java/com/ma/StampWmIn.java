package com.ma;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class StampWmIn extends ProcessFunction<TradeEvent, TradeEvent> {
  private static final long serialVersionUID = 1L;

  @Override
  public void processElement(TradeEvent e, Context ctx, Collector<TradeEvent> out) throws Exception {
    e.wmIn = ctx.timerService().currentProcessingTime();
    out.collect(e);
  }
}