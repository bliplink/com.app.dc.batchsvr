package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson2.annotation.JSONField;

public class TKols {
	
    @JSONField(name = "close_by",serialize=true)
    public String close_by;
    @JSONField(name = "create_time",serialize=true)
    public String create_time;
    @JSONField(name = "kol_user_id",serialize=true)
    public String kol_user_id = "";
    @JSONField(name = "level1_rebate",serialize=true)
    public BigDecimal level1_rebate;
    @JSONField(name = "level2_rebate",serialize=true)
    public BigDecimal level2_rebate;
    @JSONField(name = "referrer",serialize=true)
    public String referrer;
    @JSONField(name = "remark",serialize=true)
    public String remark;
    @JSONField(name = "status",serialize=true)
    public String status;
    @JSONField(name = "update_time",serialize=true)
    public String update_time;
    @JSONField(name = "invited_count",serialize=true)
    public int invited_count;//
    @JSONField(name = "volume",serialize=true)
    public BigDecimal volume;//


    @Override
    public String toString() {
        return "TKols{" +
                "close_by='" + close_by + '\'' +
                ", create_time='" + create_time + '\'' +
                ", kol_user_id='" + kol_user_id + '\'' +
                ", level1_rebate=" + level1_rebate +
                ", level2_rebate=" + level2_rebate +
                ", referrer='" + referrer + '\'' +
                ", remark='" + remark + '\'' +
                ", status='" + status + '\'' +
                ", update_time='" + update_time + '\'' +
                ", invited_count=" + invited_count +
                ", volume=" + volume +
                '}';
    }
}
