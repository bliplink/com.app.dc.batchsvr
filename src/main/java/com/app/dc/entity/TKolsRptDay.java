package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson2.annotation.JSONField;

public class TKolsRptDay {
    @JSONField(name = "invited_count",serialize=true)
    public int invited_count;
    @JSONField(name = "create_time",serialize=true)
    public String create_time;
    @JSONField(name = "fee_amount",serialize=true)
    public BigDecimal fee_amount;
    @JSONField(name = "kol_user_id",serialize=true)
    public String kol_user_id = "";
    @JSONField(name = "status",serialize=true)
    public String status;
    @JSONField(name = "trade_date",serialize=true)
    public String trade_date = "";
    @JSONField(name = "txid",serialize=true)
    public String txid;
    @JSONField(name = "update_time",serialize=true)
    public String update_time;
    @JSONField(name = "volume",serialize=true)
    public BigDecimal volume;//
}
