package com.app.dc.entity;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class LPValue {

	public String user_id;
	public String lp;
	public String securityId;
	public String marketIndex;
	public BigDecimal lpvalue;

 
	public String perpMarket;
	public long reserve_quote_amount;
	public long reserve_base_amount;

	public long tick_lower_index;
	public long tick_upper_index;
	public long lower_rate;
	public long upper_rate;
	public long sqrtPrice;
	
	public long liquidity;
	public BigDecimal tokenB;
	public BigDecimal tokenA;
	
	public BigDecimal lower_rate2;
	public BigDecimal upper_rate2;
	public BigDecimal sqrtPrice2;
	public BigDecimal liquidity2;
	public BigDecimal reserve_quote_amount2;
	public BigDecimal reserve_base_amount2;

	public BigDecimal claim_fee=BigDecimal.ZERO;

}
