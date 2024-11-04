package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;

public class TCategoryReferencePrice {
    @JSONField(name = "create_time",serialize=true)
    public String create_time;//
    @JSONField(name = "epoch",serialize=true)
    public int epoch;//
    @JSONField(name = "epoch_time",serialize=true)
    public String epoch_time;//
    @JSONField(name = "id",serialize=true)
    public String id = "";//
    @JSONField(name = "price",serialize=true)
    public BigDecimal price;//
    @JSONField(name = "symbol_category",serialize=true)
    public String symbol_category;//
    @JSONField(name = "term",serialize=true)
    public String term;//
}
