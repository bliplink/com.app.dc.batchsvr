package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;

public class TSolanaLpApr {
    @JSONField(name = "user_id",serialize=true)
    public String user_id;//
    @JSONField(name = "account_id",serialize=true)
    public String account_id;//
    @JSONField(name = "symbol",serialize=true)
    public String symbol;//
    @JSONField(name = "update_time",serialize=true)
    public String update_time;//
    @JSONField(name = "holding_period",serialize=true)
    public String holding_period;//
    @JSONField(name = "collected_fee",serialize=true)
    public BigDecimal collected_fee;//
    @JSONField(name = "avg_quote_amount",serialize=true)
    public BigDecimal avg_quote_amount;//
    @JSONField(name = "trade_date",serialize=true)
    public String trade_date;//
    @JSONField(name = "market_indicator",serialize=true)
    public String market_indicator;//
}
