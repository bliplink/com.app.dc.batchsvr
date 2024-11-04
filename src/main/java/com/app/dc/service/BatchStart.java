package com.app.dc.service;

import com.app.common.db.DBUtils;
import com.app.common.db.IDatabaseConnection;
import com.app.dc.data.DataFacade;
import com.app.common.utils.JsonUtils;
import com.app.dc.aps.ApsFacade;
import com.app.dc.price.MarketPriceFacade;
import com.app.dc.service.job.*;
import com.app.dc.util.Consts;
import com.app.dc.util.DataUtils;
import com.app.dc.utils.TradeSvrClient;
import com.app.dc.epoch.EpochFacade;
import lombok.Getter;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.app.common.utils.Schedule;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.List;

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

	@Value("${SolanaLpAprEnabled}")
	private boolean SolanaLpAprEnabled;

	@Value("${SolanaLpAprCron}")
	private String SolanaLpAprCron;

	@Value("${SolanaLpAprHourlyInterval}")
	private String SolanaLpAprHourlyInterval;

	@Value("${LPIntervalEnabled}")
	private boolean LPIntervalEnabled;

	@Value("${LPIntervalCron}")
	private String LPIntervalCron;

	@Value("${LPIntervalMultiple}")
	@Getter
	private String LPIntervalMultiple;

	@Value("${LPInterval}")
	@Getter
	private String LPInterval;

	@Value("${ConvertCurrency}")
	@Getter
	private String ConvertCurrency;

	@Value("${PointsHourEnabled}")
	private boolean pointsHourEnabled;

	@Value("${PointsHourCron}")
	private String pointsHourCron;

	@Value("${PointsDayEnabled}")
	private boolean pointsDayEnabled;

	@Value("${PointsDayCron}")
	private String pointsDayCron;

	@Autowired
	public EpochFacade epochFacade;

	@Value("${JtoIntegralEnabled}")
	private boolean JtoIntegralEnabled;

	@Value("${JtoIntegralCron}")
	private String JtoIntegralCron;

	@Value("${JtoStartDate}")
	@Getter
	private String JtoStartDate;

	@Value("${JtoEndDate}")
	@Getter
	private String JtoEndDate;

	@Getter
	@Value("${JtoAmount}")
	private BigDecimal JtoAmount;

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
		epochFacade.subscribeDPriceForTrade();
		apsFacade.init();
		dataFacade.init();
		marketPriceFacade.init();
		Thread.sleep(15000);
		log.info("BachCount:{}", Consts.BachCount);
		log.info("Schedule.Config:{}", scheduleConfig);

		log.info("MobilitySplit:{}", MobilitySplit);
		log.info("RecursionCount:{}", RecursionCount);
		log.info("ConvertCurrency:{}", ConvertCurrency);

		schedule.init(scheduleConfig);
		runYesterdaysDataJob();
		runRewardRateEnabled();
		runLpAprJob();
		runLPIntervalJob();
		runJtoIntegralJob();
		runPointsHourJob();
		runPointsDayJob();
		schedule.start();
	}

	private void runPointsHourJob() {
		log.info("pointsHourEnabled:{} pointsHourCron:{}", pointsHourEnabled, pointsHourCron);
		if (pointsHourEnabled) {
			Runnable target = () -> {
				PointsHourJob job = new PointsHourJob();
				job.setBatchStart(this);
				try {
					job.execute(null);
				} catch (JobExecutionException e) {
					log.error("runPointsHourJob execute", e);
				}
			};
			new Thread(target).start();
			schedule.updateJob(pointsHourCron, "PointsHourJob", PointsHourJob.class, this);
		}
	}

	private void runPointsDayJob() {
		log.info("pointsDayEnabled:{} pointsDayCron:{}", pointsDayEnabled, pointsDayCron);
		if (pointsDayEnabled) {

			schedule.updateJob(pointsDayCron, "PointsDayJob", PointsDayJob.class, this);
		}
	}

	private void runRewardRateEnabled() {
		log.info("RewardRateEnabled:{} RewardRateCron:{}", RewardRateEnabled, RewardRateCron);
		if (RewardRateEnabled) {
			log.info("RewardRateDay:{}", RewardRateDay);
			Consts.RewardRateDay = DataUtils.getListStr(RewardRateDay);
			Runnable target = () -> {
				RewardRateJob job = new RewardRateJob();
				job.setBatchStart(this);
				try {
					job.execute(null);
				} catch (JobExecutionException e) {
					log.error("runRewardRateEnabled execute", e);
				}
			};
			new Thread(target).start();
			schedule.updateJob(RewardRateCron, "RewardRateJob", RewardRateJob.class, this);
		}
	}

	private void runYesterdaysDataJob() {
		log.info("yesterdaysDataEnabled:{} yesterdaysDataCron:{}", yesterdaysDataEnabled, yesterdaysDataCron);
		if (yesterdaysDataEnabled) {
//			Runnable target = () -> {
//				YesterdaysDataJob job = new YesterdaysDataJob();
//				job.setBatchStart(this);
//				try {
//					job.execute(null);
//				} catch (JobExecutionException e) {
//					log.error("runYesterdaysDataJob execute", e);
//				}
//			};
//			new Thread(target).start();
			schedule.updateJob(yesterdaysDataCron, "YesterdaysDataJob", YesterdaysDataJob.class, this);
		}
	}

	private void runLpAprJob() {
		log.info("SolanaLpAprEnabled:{} SolanaLpAprCron:{} SolanaLpAprHourlyInterval:{}", SolanaLpAprEnabled,
				SolanaLpAprCron, SolanaLpAprHourlyInterval);
		if (SolanaLpAprEnabled) {
			int interval = 6;
			try {
				interval = Integer.parseInt(SolanaLpAprHourlyInterval);
			} catch (Exception e) {

			}

			String[] hourlyInterval = getSolanaLpAprHourlyInterval(interval);
			String str = JsonUtils.Serializer(hourlyInterval);
			Consts.hourlyInterval = hourlyInterval;
			log.info("hourlyInterval:{}", str);
			Runnable target = () -> {
				SolanaLpAprJob job = new SolanaLpAprJob();
				job.setBatchStart(this);
				try {
					job.execute(null);
				} catch (JobExecutionException e) {
					log.error("SolanaLpAprJob execute", e);
				}
			};
			new Thread(target).start();
			schedule.updateJob(SolanaLpAprCron, "SolanaLpAprJob", SolanaLpAprJob.class, this);
		}
	}

	private void runJtoIntegralJob() {
		log.info("runJtoIntegralJob JtoIntegralEnabled:{} LPIntervalCron:{}", JtoIntegralEnabled, JtoIntegralCron);
		if (JtoIntegralEnabled) {
			Runnable target = () -> {
				JtoIntegralJob job = new JtoIntegralJob();
				job.setBatchStart(this);
				try {
					job.execute(null);
				} catch (JobExecutionException e) {
					log.error("runJtoIntegralJob execute", e);
				}
			};
			new Thread(target).start();
			schedule.updateJob(JtoIntegralCron, "JtoIntegralJob", JtoIntegralJob.class, this);
		}
	}

	private String[] getSolanaLpAprHourlyInterval(int hourlyInterval) {
		int hourly24 = 24;
		int count = hourly24 / hourlyInterval;
		String[] hourlys = new String[count];
		for (int i = 0; i < count; i++) {
			int begin = i * hourlyInterval;
			String hourly = String.valueOf(begin);
			if (hourly.length() == 1) {
				hourly = "0" + hourly;
			}
			hourlys[i] = hourly;
		}
		return hourlys;
	}

	private void runLPIntervalJob() {
		log.info("runLPIntervalJob LPIntervalEnabled:{} LPIntervalCron:{}", LPIntervalEnabled, LPIntervalCron);
		if (LPIntervalEnabled) {
			Runnable target = () -> {
				LPIntervalJob job = new LPIntervalJob();
				job.setBatchStart(this);
				try {
					job.execute(null);
				} catch (JobExecutionException e) {
					log.error("runLPIntervalJob execute", e);
				}
			};
			new Thread(target).start();
			schedule.updateJob(LPIntervalCron, "LPIntervalJob", LPIntervalJob.class, this);
		}
	}
}
