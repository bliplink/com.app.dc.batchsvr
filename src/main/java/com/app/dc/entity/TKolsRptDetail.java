package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson2.annotation.JSONField;
public class TKolsRptDetail {
    @JSONField(name = "create_time",serialize=true)
    public String create_time;
    @JSONField(name = "fee",serialize=true)
    public BigDecimal fee;
    @JSONField(name = "fee_amount",serialize=true)
    public BigDecimal fee_amount;
    @JSONField(name = "kol_user_id",serialize=true)
    public String kol_user_id = "";
    @JSONField(name = "rebate",serialize=true)
    public BigDecimal rebate;
    @JSONField(name = "trade_date",serialize=true)
    public String trade_date = "";
    @JSONField(name = "type",serialize=true)
    public String type;
    @JSONField(name = "update_time",serialize=true)
    public String update_time;
    @JSONField(name = "user_id",serialize=true)
    public String user_id;
    @JSONField(name = "volume",serialize=true)
    public BigDecimal volume;
}
