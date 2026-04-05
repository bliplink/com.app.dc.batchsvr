package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson2.annotation.JSONField;

public class TOrdersPositionDay {
    @JSONField(name = "user_id",serialize=true)
    public String user_id = "";//
    @JSONField(name = "security_id",serialize=true)
    public String security_id = "";//
    @JSONField(name = "symbol",serialize=true)
    public String symbol = "";//
    @JSONField(name = "account_id",serialize=true)
    public String account_id = "";//
    @JSONField(name = "trade_date",serialize=true)
    public String trade_date = "";//
    @JSONField(name = "currency",serialize=true)
    public String currency = "";//
    @JSONField(name = "status",serialize=true)
    public int status = 0;//
    @JSONField(name = "position_type",serialize=true)
    public String position_type = "";//
    @JSONField(name = "leverage",serialize=true)
    public BigDecimal leverage = new BigDecimal(0);//
    @JSONField(name = "long_position",serialize=true)
    public BigDecimal long_position = new BigDecimal(0);//
    @JSONField(name = "long_average",serialize=true)
    public BigDecimal long_average = new BigDecimal(0);//
    @JSONField(name = "long_used_margin",serialize=true)
    public BigDecimal long_used_margin = new BigDecimal(0);//
    @JSONField(name = "short_position",serialize=true)
    public BigDecimal short_position = new BigDecimal(0);//
    @JSONField(name = "short_average",serialize=true)
    public BigDecimal short_average = new BigDecimal(0);//
    @JSONField(name = "short_used_margin",serialize=true)
    public BigDecimal short_used_margin = new BigDecimal(0);//
    @JSONField(name = "long_locked_position",serialize=true)
    public BigDecimal long_locked_position;//
    @JSONField(name = "short_locked_position",serialize=true)
    public BigDecimal short_locked_position = new BigDecimal(0);//
    @JSONField(name = "long_liq_price",serialize=true)
    public BigDecimal long_liq_price = new BigDecimal(0);//
    @JSONField(name = "short_liq_price",serialize=true)
    public BigDecimal short_liq_price = new BigDecimal(0);//
    @JSONField(name = "update_time",serialize=true)
    public String update_time;//
    @JSONField(name = "close_by",serialize=true)
    public String close_by;//
    @JSONField(name = "location",serialize=true)
    public String location;//
    @JSONField(name = "inf1",serialize=true)
    public String inf1;//
    @JSONField(name = "inf2",serialize=true)
    public String inf2;//
    @JSONField(name = "inf3",serialize=true)
    public String inf3;//
    @JSONField(name = "inf4",serialize=true)
    public String inf4;//
    @JSONField(name = "inf5",serialize=true)
    public String inf5;//
    @JSONField(name = "last_price",serialize=true)
    public BigDecimal last_price = new BigDecimal(0);//
    @JSONField(name = "market_indicator",serialize=true)
    public String market_indicator;//
    @JSONField(name = "rate_price",serialize=true)
    public BigDecimal rate_price;//
}
