package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;

public class TSolanaTermRewardRate {

    @JSONField(name = "apr",serialize=true)
    public BigDecimal apr;//
    @JSONField(name = "apy",serialize=true)
    public BigDecimal apy;//
    @JSONField(name = "create_time",serialize=true)
    public String create_time = "";//
    @JSONField(name = "reward_rate",serialize=true)
    public BigDecimal reward_rate;//
    @JSONField(name = "st_volume",serialize=true)
    public BigDecimal st_volume;//
    @JSONField(name = "symbol",serialize=true)
    public String symbol = "";//
    @JSONField(name = "term",serialize=true)
    public String term = "";//
    @JSONField(name = "trade_date",serialize=true)
    public String trade_date = "";//
    @JSONField(name = "tvl",serialize=true)
    public BigDecimal tvl;//
    @JSONField(name = "market_indicator",serialize=true)
    public String market_indicator;//
    @JSONField(name = "reference_price",serialize=true)
    public BigDecimal reference_price;//

}
