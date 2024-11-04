package com.app.dc.entity;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import com.alibaba.fastjson.annotation.JSONField;
 /**
   *dc_points_protocol_position_hour Po
  *
  *2024.10.28: . (v1.0.0.1) <br/>
  */
public class TPointsProtocolPositionHour {

	@JSONField(name = "close_by",serialize=true)
	public String close_by;//close_by		
	@JSONField(name = "create_time",serialize=true)
	public String create_time;//create_time		
	@JSONField(name = "currency",serialize=true)
	public String currency = "";//currency		
	@JSONField(name = "earn_amount",serialize=true)
	public BigDecimal earn_amount;//earn_amount		
	@JSONField(name = "lp_amount",serialize=true)
	public BigDecimal lp_amount;//lp_amount		
	@JSONField(name = "lp_sum_amount",serialize=true)
	public BigDecimal lp_sum_amount;//lp_sum_amount		
	@JSONField(name = "market_indicator",serialize=true)
	public String market_indicator;//market_indicator		
	@JSONField(name = "security_id",serialize=true)
	public String security_id = "";//security_id		
	@JSONField(name = "symbol",serialize=true)
	public String symbol = "";//symbol		
	@JSONField(name = "token_amount",serialize=true)
	public BigDecimal token_amount;//token_amount		
	@JSONField(name = "trade_amount",serialize=true)
	public BigDecimal trade_amount;//trade_amount		
	@JSONField(name = "trade_date",serialize=true)
	public String trade_date = "";//trade_date		
	@JSONField(name = "trade_sum_amount",serialize=true)
	public BigDecimal trade_sum_amount;//trade_sum_amount		
	@JSONField(name = "user_id",serialize=true)
	public String user_id = "";//user_id	
	@Override
	public String toString() {
		return "TPointsProtocolPositionHour [close_by=" + close_by + ", create_time=" + create_time + ", currency="
				+ currency + ", earn_amount=" + earn_amount + ", lp_amount=" + lp_amount + ", lp_sum_amount="
				+ lp_sum_amount + ", market_indicator=" + market_indicator + ", security_id=" + security_id
				+ ", symbol=" + symbol + ", token_amount=" + token_amount + ", trade_amount=" + trade_amount
				+ ", trade_date=" + trade_date + ", trade_sum_amount=" + trade_sum_amount + ", user_id=" + user_id
				+ "]";
	}
	
}
