package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;

public class TSolanaCollectFeeRecord {
    @JSONField(name = "create_time",serialize=true)
    public String create_time;//
    @JSONField(name = "fee_a",serialize=true)
    public BigDecimal fee_a;//
    @JSONField(name = "fee_amount",serialize=true)
    public BigDecimal fee_amount;//
    @JSONField(name = "fee_b",serialize=true)
    public BigDecimal fee_b;//
    @JSONField(name = "id",serialize=true)
    public String id = "";//
    @JSONField(name = "market_index",serialize=true)
    public int market_index;//
    @JSONField(name = "market_indicator",serialize=true)
    public String market_indicator;//
    @JSONField(name = "rate_lower",serialize=true)
    public BigDecimal rate_lower;//
    @JSONField(name = "rate_upper",serialize=true)
    public BigDecimal rate_upper;//
    @JSONField(name = "security_id",serialize=true)
    public String security_id;//
    @JSONField(name = "tick_lower",serialize=true)
    public BigDecimal tick_lower;//
    @JSONField(name = "tick_upper",serialize=true)
    public BigDecimal tick_upper;//
    @JSONField(name = "update_time",serialize=true)
    public String update_time;//
    @JSONField(name = "user",serialize=true)
    public String user;//
    @JSONField(name = "user_id",serialize=true)
    public String user_id;//
}
