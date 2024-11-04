package com.app.dc.service;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.app.common.db.DBUtils;
import com.app.common.db.IDatabaseConnection;
import com.app.common.utils.Schedule;
import com.app.dc.aps.ApsFacade;
import com.app.dc.data.DataFacade;
import com.app.dc.price.MarketPriceFacade;
import com.app.dc.util.Consts;
import com.app.dc.utils.TradeSvrClient;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @Description
 * @Author
 * @Date
 **/
@Slf4j
@Component
public class BatchStart implements CommandLineRunner {
	@Value("${Schedule.Config}")
	private String scheduleConfig;

	private Schedule schedule = new Schedule();

	@Autowired
	public TradeSvrClient tradeSvrClient;

	@Autowired
	public ApsFacade apsFacade;

	@Autowired
	public DataFacade dataFacade;

	@Autowired
	public MarketPriceFacade marketPriceFacade;

	@Value("${RewardRateEnabled}")
	private boolean RewardRateEnabled;

	@Value("${RewardRateCron}")
	private String RewardRateCron;

	@Value("${RewardRateDay}")
	private String RewardRateDay;

	@Getter
	@Value("${MobilitySplit}")
	private String MobilitySplit;

	@Getter
	@Value("${RecursionCount}")
	private BigDecimal RecursionCount;

	@Value("${YesterdaysDataEnabled}")
	private boolean yesterdaysDataEnabled;

	@Value("${YesterdaysDataCron}")
	private String yesterdaysDataCron;

	@Getter
	@Value("${RakeBackToChainEnabled}")
	private boolean rakeBackToChainEnabled;

	@Value("${RakeBackConvert}")
	@Getter
	private String RakeBackConvert;

	@Override
	public void run(String... args) throws Exception {
		com.gateway.connector.utils.Consts.WaitTime = 120 * 1000;
		IDatabaseConnection databaseConnection = new IDatabaseConnection() {

			@Override
			public Connection getConnection() {
				try {
					return DBUtils.dbpool.getLongConnection();
				} catch (Exception e) {
					return null;
				}
			}

			@Override
			public void freeConnection(Connection connection) {
				try {
					DBUtils.dbpool.freeConnection(connection);
				} catch (Exception e) {

				}
			}
		};

		DBUtils.setDatabaseConnection(databaseConnection);
		List<String> lt = dataFacade.getAllTSymbolCategory();
		for (String symbolCategory : lt) {
			String serverName = "Trade" + symbolCategory + "Svr";
			tradeSvrClient.serverName = "SERVER." + serverName;
			tradeSvrClient.init();
		}

		apsFacade.init();
		dataFacade.init();
		marketPriceFacade.init();
		Thread.sleep(15000);
		log.info("BachCount:{}", Consts.BachCount);
		log.info("Schedule.Config:{}", scheduleConfig);

		schedule.init(scheduleConfig);
		//runYesterdaysDataJob();

	
		schedule.start();
	}

	

//	private void runYesterdaysDataJob() {
//		log.info("yesterdaysDataEnabled:{} yesterdaysDataCron:{}", yesterdaysDataEnabled, yesterdaysDataCron);
//		if (yesterdaysDataEnabled) {
//			schedule.updateJob(yesterdaysDataCron, "YesterdaysDataJob", YesterdaysDataJob.class, this);
//		}
//	}

	
}
