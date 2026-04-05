package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson2.annotation.JSONField;

public class TUsersBalanceDay {
    @JSONField(name = "account_id",serialize=true)
    public String account_id = "";//
    @JSONField(name = "balance",serialize=true)
    public BigDecimal balance;//
    @JSONField(name = "close_by",serialize=true)
    public String close_by;//
    @JSONField(name = "currency",serialize=true)
    public String currency = "";//
    @JSONField(name = "freezed_commission",serialize=true)
    public BigDecimal freezed_commission;//
    @JSONField(name = "freezed_margin",serialize=true)
    public BigDecimal freezed_margin;//
    @JSONField(name = "is_trader",serialize=true)
    public String is_trader;//
    @JSONField(name = "location",serialize=true)
    public String location;//
    @JSONField(name = "position_type",serialize=true)
    public String position_type;//
    @JSONField(name = "security_id",serialize=true)
    public String security_id;//
    @JSONField(name = "trade_date",serialize=true)
    public String trade_date = "";//
    @JSONField(name = "update_time",serialize=true)
    public String update_time;//
    @JSONField(name = "used_margin",serialize=true)
    public BigDecimal used_margin;//
    @JSONField(name = "user_id",serialize=true)
    public String user_id = "";//
    @JSONField(name = "market_indicator",serialize=true)
    public String market_indicator;//
}
