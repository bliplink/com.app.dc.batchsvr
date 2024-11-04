package com.app.dc.data;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.app.dc.po.TSymbol;
import com.app.dc.po.TSymbolCategory;
import com.app.dc.util.Consts;
import com.app.dc.utils.AdminSvrClient;
import com.gw.common.utils.BaseApi.IMessage;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class DataFacade {
	@Value("#{'${symbolCategory}'.split('\\|')}")
	private List<String> symbolCategorys;
	@Autowired
	private AdminSvrClient adminSvrClient;
	private ConcurrentHashMap<String, Symbol> ht = new ConcurrentHashMap<String, Symbol>();
	private ConcurrentHashMap<String, TSymbolCategory> htSymbolCategory = new ConcurrentHashMap<String, TSymbolCategory>();
	private ConcurrentHashMap<String, TSymbolCategory> htNameSymbolCategory = new ConcurrentHashMap<String, TSymbolCategory>();

	private ConcurrentHashMap<String, Symbol> htSymbol2 = new ConcurrentHashMap<String, Symbol>();
	private IMessage<TSymbol> message = new IMessage<TSymbol>() {

		@Override
		public void onMessage(String topic, TSymbol t) {
			Symbol symbol = new Symbol();
			symbol.setSecurityID(String.valueOf(t.id));
			symbol.setSymbol(t.symbol);
//			symbol.setContractSize(new BigDecimal(t.contract_size));
//			symbol.setInitialMargin(new BigDecimal(t.initial_margin));
//			symbol.setPricePrecision(t.price_precision);
//			symbol.setQtyPrecision(t.qty_precision);
//			symbol.setTakerCommission(new BigDecimal(t.taker_commission));
//			symbol.setMakerCommission(new BigDecimal(t.maker_commission));
			symbol.setSymbolLevel1Category(t.symbol_level1_category);
			symbol.setSymbolLevel2Category(t.symbol_level2_category);
			symbol.setMinimumMaintainanceCr(t.minimum_maintainance_cr);
			symbol.setMinimumInitialCr(t.minimum_initial_cr);
			symbol.setExpiried(t.expiration);
			symbol.setDueDate(t.due_date);
			symbol.setProtocolFeeRate(t.protocol_fee_rate);
			ht.put(symbol.getSecurityID(), symbol);
			htSymbol2.put(symbol.getSymbol(), symbol);

		}
	};
	private IMessage<TSymbolCategory> tSymbolCategoryIMessage = new IMessage<TSymbolCategory>() {

		@Override
		public void onMessage(String topic, TSymbolCategory t) {
			 
				htSymbolCategory.put(String.valueOf(t.id), t);
				if (t.level == 1) {
					htNameSymbolCategory.put(t.symbol_category+"", t);
				}

				log.info("{}", t);
		 
		}
	};
	public List<String> getAllTSymbolCategory() {
		 
		return symbolCategorys;
	}
	public List<String> getAllSpotTSymbolCategory() {
		List<String> tmp=new ArrayList<String>();
		for (String symbolCategory : symbolCategorys) {
			TSymbolCategory sc=getTSymbolCategoryBySymbolCategory(symbolCategory);
			if(sc!=null&&"spot".equals((sc.location+"").toLowerCase())){
				tmp.add(symbolCategory);
			}
		}
		return tmp;
	}
	public void init() {
		adminSvrClient.init();
		adminSvrClient.subscribeSymbolForAdmin(message);
		adminSvrClient.subscribeSymbolCategoryForAdmin(tSymbolCategoryIMessage);
	}
	public TSymbolCategory getTSymbolCategoryById(String id) {
		return htSymbolCategory.get(id);
	}
	 
	public TSymbolCategory getTSymbolCategoryBySymbolCategory(String symbol_category) {
		return htNameSymbolCategory.get(symbol_category);
	}
	public List<Symbol> getAllSymbol() {
		List<Symbol> lt = new ArrayList<Symbol>();
		for (Map.Entry<String, Symbol> entry : htSymbol2.entrySet()) {
			Symbol symbol = entry.getValue();
			if("0".equals(symbol.getExpiried())&&!symbol.isMaturity()) {
				lt.add(symbol);
			}
		}
		return lt;
	}

	public Symbol getSymbol(String securityID) {
		Symbol symbol1 = ht.get(securityID);
		if (symbol1 != null) {
			if (symbol1.getExpiried().equals("0")&&!symbol1.isMaturity()) {
				return symbol1;
			} else {
				log.warn("securityID:{} is expiration", securityID);
			}
		}
		return null;
	}

	public Symbol getSymbol2(String symbol) {
		Symbol symbol1 = htSymbol2.get(symbol);
		if (symbol1 != null) {
			if (symbol1.getExpiried().equals("0")&&!symbol1.isMaturity()) {
				return symbol1;
			} else {
				log.warn("symbol:{} is expiration", symbol);
			}
		}
		return null;
	}

	public Symbol getSymbolAndExpiried(String symbol) {
		Symbol symbol1 = htSymbol2.get(symbol);
		if (symbol1 != null) {
			return symbol1;
		}
		return null;
	}


	public List<Symbol> getJitoSOLSymbolList(String startDate, String endDate) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date1 = sdf.parse(startDate);
		Date date2 = sdf.parse(endDate);
		List<Symbol> list = new ArrayList<>();
		Set<Map.Entry<String, Symbol>> sets = htSymbol2.entrySet();
		for (Map.Entry<String, Symbol> entry : sets) {
			Symbol symbol = entry.getValue();
			if (symbol.getSymbolLevel2Category().equals(Consts.JitoSOL)) {
				Date dueDate = sdf.parse(symbol.getDueDate());
				if (dueDate.getTime() >= date1.getTime() && dueDate.getTime() <= date2.getTime()) {
					list.add(symbol);
				}
			}
		}
		return list;
	}

}
