package com.app.dc.entity;

import java.math.BigDecimal;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

@Data
public class TSolanaHFee {
    @JSONField(name = "create_time",serialize=true)
    public String create_time = "";//
    @JSONField(name = "fee_a",serialize=true)
    public BigDecimal fee_a;//
    @JSONField(name = "fee_b",serialize=true)
    public BigDecimal fee_b;//
    @JSONField(name = "lp_value",serialize=true)
    public BigDecimal lp_value;//
    @JSONField(name = "market_indicator",serialize=true)
    public String market_indicator;//
    @JSONField(name = "market_rate",serialize=true)
    public BigDecimal market_rate;//
    @JSONField(name = "rate_price",serialize=true)
    public BigDecimal rate_price;//
    @JSONField(name = "symbol",serialize=true)
    public String symbol = "";//
    @JSONField(name = "trade_date",serialize=true)
    public String trade_date = "";//
    @JSONField(name = "tvl",serialize=true)
    public BigDecimal tvl;//
}
