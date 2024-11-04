package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;

public class TUsersTotalBalance {
    @JSONField(name = "create_time",serialize=true)
    public String create_time;//
    @JSONField(name = "realized_pnl",serialize=true)
    public BigDecimal realized_pnl;//
    @JSONField(name = "sum_realized_pnl",serialize=true)
    public BigDecimal sum_realized_pnl;//
    @JSONField(name = "today_total_close_fee",serialize=true)
    public BigDecimal today_total_close_fee;//
    @JSONField(name = "today_total_deposit_amount",serialize=true)
    public BigDecimal today_total_deposit_amount;//
    @JSONField(name = "today_total_pnl",serialize=true)
    public BigDecimal today_total_pnl;//
    @JSONField(name = "today_total_withdrawal_amount",serialize=true)
    public BigDecimal today_total_withdrawal_amount;//
    @JSONField(name = "total_balance",serialize=true)
    public BigDecimal total_balance;//
    @JSONField(name = "total_pnl",serialize=true)
    public BigDecimal total_pnl;//
    @JSONField(name = "trade_date",serialize=true)
    public String trade_date = "";//
    @JSONField(name = "user_id",serialize=true)
    public String user_id = "";//
}
