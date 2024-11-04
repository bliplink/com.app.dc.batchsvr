package com.app.dc.price;

import com.app.common.utils.JsonUtils;
import com.app.common.utils.StringUtil;
import com.app.dc.entity.MarketDataResult;
import com.app.dc.fix.message.MarketDataSnapshotFullRefresh;
import com.app.dc.utils.MDSvrClient;
import com.gw.common.utils.BaseApi.IMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class MarketPriceFacade {

	@Autowired
	private MDSvrClient mdSvrClient;
	
	private ConcurrentHashMap<String, MarketPrice> ht = new ConcurrentHashMap<String, MarketPrice>();

	private IMessage<MarketDataSnapshotFullRefresh> message = new IMessage<MarketDataSnapshotFullRefresh>() {

		@Override
		public void onMessage(String topic, MarketDataSnapshotFullRefresh t) {
			log.info("MarketDataSnapshotFullRefresh:{}",t);
			String securityId = t.getSecurityID();
			String marketIndicator = t.getMarketIndicator();
			MarketPrice mp = ht.get(securityId);
			if (mp == null) {
				mp = new MarketPrice();
				mp.setSecurityId(securityId);
				mp.setMarketIndicator(marketIndicator);
				ht.put(securityId, mp);
			}
			BigDecimal markPx = getDec(t.getMarkPrice());
			BigDecimal tradePx = getDec(t.getLastPrice());
			BigDecimal funding = StringUtil.isEmpty(t.getFee()) ? BigDecimal.ZERO : new BigDecimal(t.getFee());
			BigDecimal closePrice = getDec(t.getClosePrice());
			mp.setMarkPx(markPx);
			mp.setTradePx(tradePx);
			mp.setFunding(funding);
			mp.setClosePrice(closePrice);
		}
	};

	public void init() {

		mdSvrClient.init();

	}

	public void subscribeTradeForMD(String securityID) {

		mdSvrClient.subscribeTradeForMD(securityID, message);

	}

	public HashMap<String, MarketPrice> getMarketPriceMap() {
		HashMap<String, MarketPrice> ht = new HashMap<String, MarketPrice>();
		String s = mdSvrClient.queryTrade();
		MarketDataResult marketDataResult = JsonUtils.Deserialize(s, MarketDataResult.class);
		if (marketDataResult.data != null && marketDataResult.data.size() >0){
			for (MarketDataSnapshotFullRefresh t : marketDataResult.data){
				String securityId = t.getSecurityID();
				String marketIndicator = t.getMarketIndicator();
				MarketPrice mp = ht.get(securityId);
				if (mp == null) {
					mp = new MarketPrice();
					mp.setSecurityId(securityId);
					mp.setMarketIndicator(marketIndicator);
					ht.put(securityId, mp);
				}
				BigDecimal markPx = getDec(t.getMarkPrice());
				BigDecimal tradePx = getDec(t.getLastPrice());
				BigDecimal funding = StringUtil.isEmpty(t.getFee()) ? BigDecimal.ZERO : new BigDecimal(t.getFee());
				BigDecimal closePrice = getDec(t.getClosePrice());
				BigDecimal yield = getDec(t.getYield());
				mp.setMarkPx(markPx);
				mp.setTradePx(tradePx);
				mp.setFunding(funding);
				mp.setClosePrice(closePrice);
				mp.setYield(yield);
			}

		}
		return ht;
	}


	public List<MarketDataSnapshotFullRefresh> getTrade() {
//		HashMap<String, MarketDataSnapshotFullRefresh> ht = new HashMap<>();
		String s = mdSvrClient.queryTrade();
		List<MarketDataSnapshotFullRefresh> list = new ArrayList<>();
		MarketDataResult marketDataResult = JsonUtils.Deserialize(s, MarketDataResult.class);
		if (marketDataResult.data != null && marketDataResult.data.size() >0){
//			for (MarketDataSnapshotFullRefresh t : marketDataResult.data){
//				String securityId = t.getSecurityID();
//				String marketIndicator = t.getMarketIndicator();
//				ht.put(securityId,t);
//			}
			list.addAll(marketDataResult.data);

		}
		return list;
	}

	public MarketPrice getMPrice(String securityID) {
		MarketPrice mp = ht.get(securityID);
		if (mp == null) {
			synchronized (securityID.intern()) {

				subscribeTradeForMD(securityID);
				try {
					Thread.sleep(500);
					mp = ht.get(securityID);

				} catch (InterruptedException e) {

				}

			}
		}
		return mp;
	}

	public BigDecimal getFunding(String securityID) {
		BigDecimal funding = BigDecimal.ZERO;
		MarketPrice mp = ht.get(securityID);
		if (mp != null) {
			funding = mp.getFunding() != null ? mp.getFunding() : BigDecimal.ZERO;
		}
		return funding;
	}

	public BigDecimal getMarketPrice(String securityID) {
		MarketPrice mp = getMPrice(securityID);
		BigDecimal px = null;
		if (mp != null) {
			if (mp.getMarkPx() != null) {
				px = mp.getMarkPx();
			} else if (mp.getTradePx() != null) {
				px = mp.getTradePx();
			}
		}
		log.debug("{} getMarketPrice:{}[{}]", securityID, px, mp);
		return px;
	}

	public static BigDecimal getDec(String s){
		BigDecimal b = null;
		if (s != null && s.length() >0){
			b = new BigDecimal(s);
		}
		return b;
	}
}
