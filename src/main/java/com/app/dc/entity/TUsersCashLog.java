package com.app.dc.entity;

import com.alibaba.fastjson2.annotation.JSONField;

import java.math.BigDecimal;

public class TUsersCashLog {
    @JSONField(name = "account_id",serialize=true)
    public String account_id = "";
    @JSONField(name = "address",serialize=true)
    public String address;
    @JSONField(name = "amount",serialize=true)
    public BigDecimal amount;
    @JSONField(name = "cash_type",serialize=true)
    public String cash_type;
    @JSONField(name = "close_by",serialize=true)
    public String close_by;
    @JSONField(name = "confirm_num",serialize=true)
    public int confirm_num;
    @JSONField(name = "create_time",serialize=true)
    public String create_time;
    @JSONField(name = "currency",serialize=true)
    public String currency;
    @JSONField(name = "fee_amount",serialize=true)
    public BigDecimal fee_amount;
    @JSONField(name = "fee_type",serialize=true)
    public String fee_type;
    @JSONField(name = "fee_value",serialize=true)
    public String fee_value;
    @JSONField(name = "id",serialize=true)
    public String id = "";
    @JSONField(name = "location",serialize=true)
    public String location;
    @JSONField(name = "msg",serialize=true)
    public String msg;
    @JSONField(name = "nonce",serialize=true)
    public int nonce;
    @JSONField(name = "remark",serialize=true)
    public String remark;
    @JSONField(name = "side",serialize=true)
    public int side;
    @JSONField(name = "status",serialize=true)
    public int status;
    @JSONField(name = "transaction_hash",serialize=true)
    public String transaction_hash;
    @JSONField(name = "txid",serialize=true)
    public String txid = "";
    @JSONField(name = "update_time",serialize=true)
    public String update_time;
    @JSONField(name = "user_id",serialize=true)
    public String user_id = "";
    @JSONField(name = "market_indicator",serialize=true)
    public String market_indicator;//
}
