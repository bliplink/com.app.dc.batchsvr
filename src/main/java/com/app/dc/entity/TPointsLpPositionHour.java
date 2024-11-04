package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;
 /**
   *dc_points_lp_position_hour Po
  *
  *2024.10.28: . (v1.0.0.1) <br/>
  */
public class TPointsLpPositionHour {

		@JSONField(name = "account_id",serialize=true)
		public String account_id = "";//account_id		
		@JSONField(name = "close_by",serialize=true)
		public String close_by;//close_by		
		@JSONField(name = "create_time",serialize=true)
		public String create_time;//create_time		
		@JSONField(name = "currency",serialize=true)
		public String currency = "";//currency		
		@JSONField(name = "market_indicator",serialize=true)
		public String market_indicator;//market_indicator		
		@JSONField(name = "rate",serialize=true)
		public BigDecimal rate;//rate		
		@JSONField(name = "reserve_base_amount",serialize=true)
		public BigDecimal reserve_base_amount;//reserve_base_amount		
		@JSONField(name = "reserve_quote_amount",serialize=true)
		public BigDecimal reserve_quote_amount;//reserve_quote_amount		
		@JSONField(name = "security_id",serialize=true)
		public String security_id = "";//security_id		
		@JSONField(name = "symbol",serialize=true)
		public String symbol = "";//symbol		
		@JSONField(name = "token_a",serialize=true)
		public BigDecimal token_a;//token_a		
		@JSONField(name = "token_amount",serialize=true)
		public BigDecimal token_amount;//token_amount		
		@JSONField(name = "token_b",serialize=true)
		public BigDecimal token_b;//token_b		
		@JSONField(name = "trade_date",serialize=true)
		public String trade_date = "";//trade_date		
		@JSONField(name = "user_id",serialize=true)
		public String user_id = "";//user_id		
	}
