package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

@Data
public class TSolanaLpRecord {

    @JSONField(name = "user_id",serialize=true)
    public String user_id;//
    @JSONField(name = "account_id",serialize=true)
    public String account_id;//
    @JSONField(name = "currency",serialize=true)
    public String currency;//
    @JSONField(name = "security_id",serialize=true)
    public String security_id;//
    @JSONField(name = "symbol",serialize=true)
    public String symbol;//
    @JSONField(name = "direction",serialize=true)
    public int direction;//
    @JSONField(name = "margin_index",serialize=true)
    public int margin_index;//
    @JSONField(name = "delta_base_asset_amount",serialize=true)
    public BigDecimal delta_base_asset_amount;//
    @JSONField(name = "delta_quote_asset_amount",serialize=true)
    public BigDecimal delta_quote_asset_amount;//
    @JSONField(name = "tick_lower",serialize=true)
    public BigDecimal tick_lower;//
    @JSONField(name = "tick_upper",serialize=true)
    public BigDecimal tick_upper;//
    @JSONField(name = "rate_lower",serialize=true)
    public BigDecimal rate_lower;//
    @JSONField(name = "rate_upper",serialize=true)
    public BigDecimal rate_upper;//
    @JSONField(name = "margin_amount",serialize=true)
    public BigDecimal margin_amount;//
    @JSONField(name = "minted_quote_amount",serialize=true)
    public BigDecimal minted_quote_amount;//
    @JSONField(name = "liquidity_amount",serialize=true)
    public BigDecimal liquidity_amount;//
    @JSONField(name = "ts",serialize=true)
    public String ts;//
    @JSONField(name = "create_time",serialize=true)
    public String create_time;//
    @JSONField(name = "update_time",serialize=true)
    public String update_time;//
    @JSONField(name = "total_quote_asset_amount",serialize=true)
    public BigDecimal total_quote_asset_amount;//
    @JSONField(name = "total_margin_amount",serialize=true)
    public BigDecimal total_margin_amount;//
    @JSONField(name = "is_active",serialize=true)
    public int is_active = 1;//
    @JSONField(name = "social_loss",serialize=true)
    public BigDecimal social_loss;//
    @JSONField(name = "social_loss_in_margin",serialize=true)
    public BigDecimal social_loss_in_margin;//
    @JSONField(name = "txid",serialize=true)
    public String txid = "";//
    @JSONField(name = "market_indicator",serialize=true)
    public String market_indicator;//
}
