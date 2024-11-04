package com.app.dc.data;

import java.math.BigDecimal;
import java.util.Date;

import com.app.dc.utils.DateUtil;

import com.alibaba.fastjson.annotation.JSONField;

public class Symbol {
	private String securityID;//

	private String symbol;
	private int pricePrecision;//		
	private BigDecimal maxOrderQty;//		

	 
	private BigDecimal contractSize;//		
	private BigDecimal initialMargin;//		
	private BigDecimal maintMargin;//		
 		
	private BigDecimal takerCommission;//taker		
	private BigDecimal makerCommission;//maker

	private String symbolLevel1Category;//
	private String symbolLevel2Category;//

	private BigDecimal minimumInitialCr;// icr
	private BigDecimal  minimumMaintainanceCr; //mcr
	 
	public BigDecimal protocolFeeRate;//
	public BigDecimal getProtocolFeeRate() {
		return protocolFeeRate;
	}

	public void setProtocolFeeRate(BigDecimal protocolFeeRate) {
		this.protocolFeeRate = protocolFeeRate;
	}

	private int qtyPrecision;//
	private String expiried;

	public String getDueDate() {
		return dueDate;
	}

	public void setDueDate(String dueDate) {
		this.dueDate = dueDate;
	}

	private String dueDate;

	public String getSecurityID() {
		return securityID;
	}

	public void setSecurityID(String securityID) {
		this.securityID = securityID;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public int getPricePrecision() {
		return pricePrecision;
	}

	public void setPricePrecision(int pricePrecision) {
		this.pricePrecision = pricePrecision;
	}

	 
	public int getQtyPrecision() {
		return qtyPrecision;
	}

	public void setQtyPrecision(int qtyPrecision) {
		this.qtyPrecision = qtyPrecision;
	}

	public BigDecimal getMaxOrderQty() {
		return maxOrderQty;
	}

	public void setMaxOrderQty(BigDecimal maxOrderQty) {
		this.maxOrderQty = maxOrderQty;
	}

	public BigDecimal getContractSize() {
		return contractSize;
	}

	public void setContractSize(BigDecimal contractSize) {
		this.contractSize = contractSize;
	}

	public BigDecimal getInitialMargin() {
		return initialMargin;
	}

	public void setInitialMargin(BigDecimal initialMargin) {
		this.initialMargin = initialMargin;
	}

	public BigDecimal getMaintMargin() {
		return maintMargin;
	}

	public void setMaintMargin(BigDecimal maintMargin) {
		this.maintMargin = maintMargin;
	}

	public BigDecimal getTakerCommission() {
		return takerCommission;
	}

	public void setTakerCommission(BigDecimal takerCommission) {
		this.takerCommission = takerCommission;
	}

	public BigDecimal getMakerCommission() {
		return makerCommission;
	}

	public void setMakerCommission(BigDecimal makerCommission) {
		this.makerCommission = makerCommission;
	}


	public String getSymbolLevel1Category() {
		return symbolLevel1Category;
	}

	public void setSymbolLevel1Category(String symbolLevel1Category) {
		this.symbolLevel1Category = symbolLevel1Category;
	}
	public String getSymbolLevel2Category() {
		return symbolLevel2Category;
	}

	public void setSymbolLevel2Category(String symbolLevel2Category) {
		this.symbolLevel2Category = symbolLevel2Category;
	}

	public void setMinimumInitialCr(BigDecimal minimumInitialCr) {
		this.minimumInitialCr = minimumInitialCr;
	}

	public BigDecimal getMinimumMaintainanceCr() {
		return minimumMaintainanceCr;
	}

	public void setMinimumMaintainanceCr(BigDecimal minimumMaintainanceCr) {
		this.minimumMaintainanceCr = minimumMaintainanceCr;
	}
	public String getExpiried() {
		return expiried;
	}

	public void setExpiried(String expiried) {
		this.expiried = expiried;
	}

	public boolean isMaturity() {
		boolean flag = false;
		long diffTime = new Date().getTime() - DateUtil.getDateLong2(getDueDate());
		if (diffTime >= 0) {
			flag = true;
		}
		return flag;
	}
	@Override
	public String toString() {
		return "Symbol{" +
				"securityID='" + securityID + '\'' +
				", symbol='" + symbol + '\'' +
				", pricePrecision=" + pricePrecision +
				", maxOrderQty=" + maxOrderQty +
				", contractSize=" + contractSize +
				", initialMargin=" + initialMargin +
				", maintMargin=" + maintMargin +
				", takerCommission=" + takerCommission +
				", makerCommission=" + makerCommission +
				", symbolLevel1Category='" + symbolLevel1Category + '\'' +
				", symbolLevel2Category='" + symbolLevel2Category + '\'' +
				", minimumInitialCr=" + minimumInitialCr +
				", minimumMaintainanceCr=" + minimumMaintainanceCr +
				", qtyPrecision=" + qtyPrecision +
				'}';
	}
}
