package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;
 /**
   *dc_points_ethena_hour Po
  *
  *2024.10.28: . (v1.0.0.1) <br/>
  */
public class TPointsUsersHour {

		@JSONField(name = "close_by",serialize=true)
		public String close_by;//close_by		
		@JSONField(name = "create_time",serialize=true)
		public String create_time;//create_time	 		
		@JSONField(name = "market_indicator",serialize=true)
		public String market_indicator;//market_indicator		
		@JSONField(name = "token_amount",serialize=true)
		public BigDecimal token_amount=BigDecimal.ZERO;//token_amount		
		@JSONField(name = "trade_date",serialize=true)
		public String trade_date = "";//trade_date		
		@JSONField(name = "user_id",serialize=true)
		public String user_id = "";//user_id		
	}
