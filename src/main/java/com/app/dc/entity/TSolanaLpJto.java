package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;

public class TSolanaLpJto {
    @JSONField(name = "create_time",serialize=true)
    public String create_time = "";//
    @JSONField(name = "jto",serialize=true)
    public BigDecimal jto;//
    @JSONField(name = "jto_total_supply",serialize=true)
    public BigDecimal jto_total_supply;//
    @JSONField(name = "total_collect_claim_fee",serialize=true)
    public BigDecimal total_collect_claim_fee;//
    @JSONField(name = "user_collect_claim_fee",serialize=true)
    public BigDecimal user_collect_claim_fee;//
    @JSONField(name = "user_id",serialize=true)
    public String user_id = "";//
}
