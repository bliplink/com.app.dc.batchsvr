package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson2.annotation.JSONField;

public class TSymbolPriceDay {
    @JSONField(name = "create_time",serialize=true)
    public String create_time;
    @JSONField(name = "price",serialize=true)
    public BigDecimal price;
    @JSONField(name = "symbol",serialize=true)
    public String symbol = "";
    @JSONField(name = "trade_date",serialize=true)
    public String trade_date = "";
    @JSONField(name = "update_time",serialize=true)
    public String update_time;
}
