package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson2.annotation.JSONField;
import lombok.Data;

@Data
public class TOrdersExecorders {
    @JSONField(name = "account_id",serialize=true)
    public String account_id = "";//
    @JSONField(name = "close_by",serialize=true)
    public String close_by;//
    @JSONField(name = "create_time",serialize=true)
    public String create_time;//
    @JSONField(name = "currency",serialize=true)
    public String currency = "";//
    @JSONField(name = "exec_id",serialize=true)
    public String exec_id = "";//
    @JSONField(name = "exec_type",serialize=true)
    public String exec_type;//7锛歍aker锛�6锛歁aker
    @JSONField(name = "fee",serialize=true)
    public BigDecimal fee;//
    @JSONField(name = "info1",serialize=true)
    public String info1;//
    @JSONField(name = "last_px",serialize=true)
    public BigDecimal last_px;//
    @JSONField(name = "last_qty",serialize=true)
    public BigDecimal last_qty;//
    @JSONField(name = "last_yield",serialize=true)
    public BigDecimal last_yield;//
    @JSONField(name = "location",serialize=true)
    public String location;//
    @JSONField(name = "Maker",serialize=true)
    public int Maker;//
    @JSONField(name = "market_indicator",serialize=true)
    public String market_indicator;//
    @JSONField(name = "oc_type",serialize=true)
    public String oc_type;//
    @JSONField(name = "order_id",serialize=true)
    public String order_id;//
    @JSONField(name = "order_txid",serialize=true)
    public String order_txid;//
    @JSONField(name = "position_type",serialize=true)
    public String position_type;//
    @JSONField(name = "rate",serialize=true)
    public BigDecimal rate;//
    @JSONField(name = "realized_Pnl",serialize=true)
    public BigDecimal realized_Pnl;//
    @JSONField(name = "security_id",serialize=true)
    public String security_id;//
    @JSONField(name = "side",serialize=true)
    public String side;//
    @JSONField(name = "symbol",serialize=true)
    public String symbol;//
    @JSONField(name = "transact_time",serialize=true)
    public String transact_time;//
    @JSONField(name = "update_time",serialize=true)
    public String update_time;//
    @JSONField(name = "user_id",serialize=true)
    public String user_id = "";//
}
