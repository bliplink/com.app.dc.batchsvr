package com.app.dc.entity;

import java.math.BigDecimal;
import java.util.List;

public class LpSymbolMobility {
    public String symbol;
    public BigDecimal maxLimit;
    public BigDecimal minLimit;

    @Override
    public String toString() {
        return "LpSymbolMobility{" +
                "symbol='" + symbol + '\'' +
                ", maxLimit=" + maxLimit +
                ", minLimit=" + minLimit +
                '}';
    }
}
