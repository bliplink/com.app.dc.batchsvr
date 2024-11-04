package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;
 /**
   *dc_points_trade_position_hour Po
  *
  *2024.10.28: . (v1.0.0.1) <br/>
  */
public class TPointsTradePositionHour {

		@JSONField(name = "account_id",serialize=true)
		public String account_id = "";//account_id		
		@JSONField(name = "balance",serialize=true)
		public BigDecimal balance = new BigDecimal(0);//balance		
		@JSONField(name = "close_by",serialize=true)
		public String close_by;//close_by		
		@JSONField(name = "create_time",serialize=true)
		public String create_time;//create_time		
		@JSONField(name = "currency",serialize=true)
		public String currency = "";//currency		
		@JSONField(name = "market_indicator",serialize=true)
		public String market_indicator;//market_indicator		
		@JSONField(name = "rate",serialize=true)
		public BigDecimal rate = new BigDecimal(0);//rate		
		@JSONField(name = "security_id",serialize=true)
		public String security_id = "";//security_id		
		@JSONField(name = "st",serialize=true)
		public BigDecimal st = new BigDecimal(0);//st		
		@JSONField(name = "symbol",serialize=true)
		public String symbol = "";//symbol		
		@JSONField(name = "token_amount",serialize=true)
		public BigDecimal token_amount;//token_amount		
		@JSONField(name = "trade_date",serialize=true)
		public String trade_date = "";//trade_date		
		@JSONField(name = "user_id",serialize=true)
		public String user_id = "";//user_id		
		@JSONField(name = "yt",serialize=true)
		public BigDecimal yt = new BigDecimal(0);//yt		
	}
