package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;

public class TSolanaLpJtoFee {
    @JSONField(name = "claim_fee",serialize=true)
    public BigDecimal claim_fee;//
    @JSONField(name = "cllect_fee",serialize=true)
    public BigDecimal cllect_fee;//
    @JSONField(name = "create_time",serialize=true)
    public String create_time = "";//
    @JSONField(name = "lp",serialize=true)
    public String lp = "";//
    @JSONField(name = "market_index",serialize=true)
    public String market_index = "";//
    @JSONField(name = "perp_market",serialize=true)
    public String perp_market;//
    @JSONField(name = "symbol",serialize=true)
    public String symbol = "";//
    @JSONField(name = "trade_date",serialize=true)
    public String trade_date = "";//
    @JSONField(name = "user_id",serialize=true)
    public String user_id = "";//
}
