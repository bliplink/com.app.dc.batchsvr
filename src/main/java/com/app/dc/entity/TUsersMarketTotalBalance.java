package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;

public class TUsersMarketTotalBalance {
    @JSONField(name = "create_time",serialize=true)
    public String create_time;//
    @JSONField(name = "exchange_rate",serialize=true)
    public BigDecimal exchange_rate;//
    @JSONField(name = "market_indicator",serialize=true)
    public String market_indicator = "";//
    @JSONField(name = "realized_pnl",serialize=true)
    public BigDecimal realized_pnl;//
    @JSONField(name = "sum_balance",serialize=true)
    public BigDecimal sum_balance;//
    @JSONField(name = "sum_realized_pnl",serialize=true)
    public BigDecimal sum_realized_pnl;//
    @JSONField(name = "today_total_close_fee",serialize=true)
    public BigDecimal today_total_close_fee;//
    @JSONField(name = "today_total_deposit_amount",serialize=true)
    public BigDecimal today_total_deposit_amount;//
    @JSONField(name = "today_total_withdrawal_amount",serialize=true)
    public BigDecimal today_total_withdrawal_amount;//
    @JSONField(name = "trade_date",serialize=true)
    public String trade_date = "";//
    @JSONField(name = "unrealized_pnl",serialize=true)
    public BigDecimal unrealized_pnl;//
    @JSONField(name = "user_id",serialize=true)
    public String user_id = "";//
}
