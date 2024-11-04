package com.app.dc.price;

import java.math.BigDecimal;

public class MarketPrice {
	private String securityId;
	private String marketIndicator;
	private BigDecimal bid;
	private BigDecimal ask;
	private BigDecimal markPx;
	private BigDecimal tradePx;
	private BigDecimal funding;

	private BigDecimal yield;

	private BigDecimal closePrice;

	public String getSecurityId() {
		return securityId;
	}

	public void setSecurityId(String securityId) {
		this.securityId = securityId;
	}

	public String getMarketIndicator() {
		return marketIndicator;
	}

	public void setMarketIndicator(String marketIndicator) {
		this.marketIndicator = marketIndicator;
	}

	public BigDecimal getFunding() {
		return funding;
	}

	public void setFunding(BigDecimal funding) {
		this.funding = funding;
	}

	public BigDecimal getBid() {
		return bid;
	}

	public void setBid(BigDecimal bid) {
		this.bid = bid;
	}

	public BigDecimal getAsk() {
		return ask;
	}

	public void setAsk(BigDecimal ask) {
		this.ask = ask;
	}

	public BigDecimal getMarkPx() {
		return markPx;
	}

	public void setMarkPx(BigDecimal markPx) {
		this.markPx = markPx;
	}

	public BigDecimal getTradePx() {
		return tradePx;
	}

	public void setTradePx(BigDecimal tradePx) {
		this.tradePx = tradePx;
	}

	public BigDecimal getClosePrice() {
		return closePrice;
	}

	public void setClosePrice(BigDecimal closePrice) {
		this.closePrice = closePrice;
	}

	public BigDecimal getYield() {
		return yield;
	}

	public void setYield(BigDecimal yield) {
		this.yield = yield;
	}

	@Override
	public String toString() {
		return "MarketPrice{" +
				"securityId='" + securityId + '\'' +
				", marketIndicator='" + marketIndicator + '\'' +
				", bid=" + bid +
				", ask=" + ask +
				", markPx=" + markPx +
				", tradePx=" + tradePx +
				", funding=" + funding +
				", yield=" + yield +
				", closePrice=" + closePrice +
				'}';
	}
}
