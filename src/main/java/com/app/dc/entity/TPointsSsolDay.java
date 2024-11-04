package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;
 /**
   *dc_points_ssol_day Po
  *
  *2024.10.28: . (v1.0.0.1) <br/>
  */
public class TPointsSsolDay {

		@JSONField(name = "close_by",serialize=true)
		public String close_by;//close_by		
		@JSONField(name = "create_time",serialize=true)
		public String create_time;//create_time		
		@JSONField(name = "currency",serialize=true)
		public String currency = "";//currency		
		@JSONField(name = "market_indicator",serialize=true)
		public String market_indicator;//market_indicator		
		@JSONField(name = "points",serialize=true)
		public BigDecimal points;//points		
		@JSONField(name = "ratio",serialize=true)
		public BigDecimal ratio;//ratio		
		@JSONField(name = "real_points",serialize=true)
		public BigDecimal real_points;//real_points		
		@JSONField(name = "status",serialize=true)
		public String status;//status		
		@JSONField(name = "trade_date",serialize=true)
		public String trade_date = "";//trade_date		
		@JSONField(name = "txid",serialize=true)
		public String txid;//txid		
		@JSONField(name = "user_id",serialize=true)
		public String user_id = "";//user_id
	    @JSONField(name = "sum_token_amount",serialize=true)
	    public BigDecimal sum_token_amount;//
	}
