package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

@Data
public class TSolanaSymbolLpMobility {
    @JSONField(name = "create_time",serialize=true)
    public String create_time = "";//
    @JSONField(name = "lower_limit",serialize=true)
    public BigDecimal lower_limit;//
    @JSONField(name = "mobility",serialize=true)
    public BigDecimal mobility;//
    @JSONField(name = "symbol",serialize=true)
    public String symbol = "";//
    @JSONField(name = "trade_date",serialize=true)
    public String trade_date = "";//
    @JSONField(name = "upper_limit",serialize=true)
    public BigDecimal upper_limit;//
}
