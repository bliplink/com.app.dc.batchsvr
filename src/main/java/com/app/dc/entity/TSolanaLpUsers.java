package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;

public class TSolanaLpUsers {

	@JSONField(name = "txid",serialize=true)
	public String txid;//txid		
	@JSONField(name = "ts",serialize=true)
	public String ts;//ts		
	@JSONField(name = "lp_authority",serialize=true)
	public String lp_authority = "";//lp_authority		
	@JSONField(name = "lp",serialize=true)
	public String lp = "";//lp		
	@JSONField(name = "sub_account_id",serialize=true)
	public BigDecimal sub_account_id;//sub_account_id		
	@JSONField(name = "security_id",serialize=true)
	public String security_id;//security_id		
	@JSONField(name = "is_active",serialize=true)
	public int is_active = 1;//is_active		
	@JSONField(name = "create_time",serialize=true)
	public String create_time;//create_time		
	@JSONField(name = "update_time",serialize=true)
	public String update_time;//update_time		
	@JSONField(name = "is_delete",serialize=true)
	public String is_delete;//is_delete		
	@JSONField(name = "market_indicator",serialize=true)
	public String market_indicator;//market_indicator	
}
