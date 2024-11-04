package com.app.dc.aps;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.app.dc.fix.message.MarketDataSnapshotFullRefresh;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.app.common.utils.JsonUtils;
import com.app.dc.data.DataFacade;
import com.app.dc.po.SolanaCategoryDPrice;
import com.app.dc.utils.ApsSvrClient;
import com.gw.common.utils.BaseApi;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ApsFacade {

	@Autowired
	private ApsSvrClient apsSvrClient;

	@Getter
	private ConcurrentHashMap<String,MarketDataSnapshotFullRefresh> exchangeRateHm = new ConcurrentHashMap<>();


	public void init() {

		log.info("ApsSvrClient init");
		apsSvrClient.init();
		apsSvrClient.subscribeMarkPriceForAPS(markPriceMessage);
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {

		}
	}

	BaseApi.IMessage<MarketDataSnapshotFullRefresh> markPriceMessage = new BaseApi.IMessage<MarketDataSnapshotFullRefresh>() {

		@Override
		public void onMessage(String topic, MarketDataSnapshotFullRefresh t) {

			exchangeRateHm.put(t.getSecurityID(),t);
		}
	};
}
