package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;

public class TSolanaHLpValue {
    @JSONField(name = "create_time",serialize=true)
    public String create_time = "";//
    @JSONField(name = "liquidity",serialize=true)
    public BigDecimal liquidity;//
    @JSONField(name = "liquidity2",serialize=true)
    public BigDecimal liquidity2;//
    @JSONField(name = "lower_rate",serialize=true)
    public BigDecimal lower_rate;//
    @JSONField(name = "lower_rate2",serialize=true)
    public BigDecimal lower_rate2;//
    @JSONField(name = "lp",serialize=true)
    public String lp = "";//
    @JSONField(name = "lp_value",serialize=true)
    public BigDecimal lp_value;//
    @JSONField(name = "market_index",serialize=true)
    public String market_index = "";//
    @JSONField(name = "perp_market",serialize=true)
    public String perp_market;//
    @JSONField(name = "reserve_base_amount",serialize=true)
    public BigDecimal reserve_base_amount;//
    @JSONField(name = "reserve_base_amount2",serialize=true)
    public BigDecimal reserve_base_amount2;//
    @JSONField(name = "reserve_quote_amount",serialize=true)
    public BigDecimal reserve_quote_amount;//
    @JSONField(name = "reserve_quote_amount2",serialize=true)
    public BigDecimal reserve_quote_amount2;//
    @JSONField(name = "sqrt_price",serialize=true)
    public BigDecimal sqrt_price;//
    @JSONField(name = "sqrt_price2",serialize=true)
    public BigDecimal sqrt_price2;//
    @JSONField(name = "symbol",serialize=true)
    public String symbol = "";//
    @JSONField(name = "tick_lower_index",serialize=true)
    public BigDecimal tick_lower_index;//
    @JSONField(name = "tick_upper_index",serialize=true)
    public BigDecimal tick_upper_index;//
    @JSONField(name = "token_a",serialize=true)
    public BigDecimal token_a;//
    @JSONField(name = "token_b",serialize=true)
    public BigDecimal token_b;//
    @JSONField(name = "trade_date",serialize=true)
    public String trade_date = "";//
    @JSONField(name = "upper_rate",serialize=true)
    public BigDecimal upper_rate;//
    @JSONField(name = "upper_rate2",serialize=true)
    public BigDecimal upper_rate2;//
    @JSONField(name = "user_id",serialize=true)
    public String user_id = "";//
    @JSONField(name = "market_indicator",serialize=true)
    public String market_indicator;//
}
