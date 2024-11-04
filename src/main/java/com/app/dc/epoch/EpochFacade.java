package com.app.dc.epoch;

import com.app.common.utils.JsonUtils;
import com.app.dc.data.DataFacade;
import com.app.dc.po.SolanaCategoryDPrice;
import com.app.dc.utils.TradeSvrClient;
import com.gw.common.utils.BaseApi;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class EpochFacade {

	@Autowired
	private TradeSvrClient tradeSvrClient;

	@Autowired
	private DataFacade dataFacade;
	private ConcurrentHashMap<String, SolanaCategoryDPrice> epochPriceHm = new ConcurrentHashMap<>();
	private ConcurrentHashMap<String, SolanaCategoryDPrice> epochSecurityIdPriceHm = new ConcurrentHashMap<>();

	public void subscribeDPriceForTrade() {
		log.info("TradeSvrClient init");
		List<String> lt = dataFacade.getAllTSymbolCategory();
		for (String symbolCategory : lt) {
			String serverName = "Trade" + symbolCategory + "Svr";
			tradeSvrClient.subscribeDPriceForTrade(serverName,new BaseApi.IMessage<SolanaCategoryDPrice>() {
				@Override
				public void onMessage(String s, SolanaCategoryDPrice solanaCategoryDPrice) {
					log.info("topic:{}, content:{}", s, JsonUtils.Serializer(solanaCategoryDPrice));
					if (solanaCategoryDPrice != null) {
						epochPriceHm.put(solanaCategoryDPrice.symbol_category, solanaCategoryDPrice);
						epochSecurityIdPriceHm.put(solanaCategoryDPrice.security_id, solanaCategoryDPrice);
					}
				}
			});
		}
	}

	public SolanaCategoryDPrice getDPrice(String symbol_category) {
		return epochPriceHm.get(symbol_category);
	}

	public SolanaCategoryDPrice getDPriceBySecurityId(String security_id) {
		SolanaCategoryDPrice dprice=epochSecurityIdPriceHm.get(security_id);
		log.info("getDPriceBySecurityId:{},{}",security_id,dprice);
		return dprice;
	}

}
