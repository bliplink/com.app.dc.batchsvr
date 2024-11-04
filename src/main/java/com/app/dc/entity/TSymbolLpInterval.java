package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;

public class TSymbolLpInterval {
    @JSONField(name = "symbol",serialize=true)
    public String symbol = "";//
    @JSONField(name = "create_time",serialize=true)
    public String create_time = "";//
    @JSONField(name = "multiples",serialize=true)
    public BigDecimal multiples = new BigDecimal(0);//
    @JSONField(name = "lower",serialize=true)
    public BigDecimal lower;//
    @JSONField(name = "upper",serialize=true)
    public BigDecimal upper;//
    @JSONField(name = "yield",serialize=true)
    public BigDecimal yield;//


    @Override
    public String toString() {
        return "TSymbolLpInterval{" +
                "symbol='" + symbol + '\'' +
                ", create_time='" + create_time + '\'' +
                ", multiples=" + multiples +
                ", lower=" + lower +
                ", upper=" + upper +
                ", yield=" + yield +
                '}';
    }
}
