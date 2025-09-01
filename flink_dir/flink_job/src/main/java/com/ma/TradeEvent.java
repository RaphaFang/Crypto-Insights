package com.ma;

import java.io.Serializable;
import java.math.BigDecimal;

public class TradeEvent implements Serializable {
    public String eventType; // e
    public long eventTime; // E (ms)
    public String symbol; // s
    public long tradeId; // t
    public BigDecimal price; // p
    public BigDecimal quantity; // q
    public long tradeTime; // T (ms)
    public boolean isBuyerMarketMaker; // m
    public boolean ignore; // M

    @Override
    public String toString() {
        return "TradeEvent{" +
                "eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                ", symbol='" + symbol + '\'' +
                ", tradeId=" + tradeId +
                ", price=" + price +
                ", quantity=" + quantity +
                ", tradeTime=" + tradeTime +
                ", isBuyerMarketMaker=" + isBuyerMarketMaker +
                ", ignore=" + ignore +
                '}';
    }
}
