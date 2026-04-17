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
import com.app.dc.service.externalsource.ExternalSourceFacade;
import com.app.dc.service.job.ExternalSourceDigestReportJob;
import com.app.dc.service.job.ExternalSourceDispatchJob;
import com.app.dc.service.job.ExternalSourceNormalizeJob;
import com.app.dc.service.job.FmzDiscoverJob;
import com.app.dc.service.job.GitHubDiscoverJob;
import com.app.dc.service.job.StrategySystemDailyReportJob;
import com.app.dc.service.job.TradingViewDiscoverJob;
import com.app.dc.service.systemreport.StrategySystemDailyReportService;
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

	@Autowired
	private ExternalSourceFacade externalSourceFacade;

	@Autowired
	private StrategySystemDailyReportService strategySystemDailyReportService;

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

	@Value("${clickhouse.default:}")
	private String clickHouseDefault;

	@Value("${dbpool.default:}")
	private String dbPoolDefault;

	@Value("${external.source.enabled:true}")
	private boolean externalSourceEnabled;

	@Value("${external.source.dispatch.enabled:false}")
	private boolean externalSourceDispatchEnabled;

	@Value("${external.tradingview.enabled:true}")
	private boolean tradingViewEnabled;

	@Value("${external.fmz.enabled:true}")
	private boolean fmzEnabled;

	@Value("${external.github.enabled:true}")
	private boolean gitHubEnabled;

	@Value("${external.tradingview.cron:0 0 0/2 * * ?}")
	private String tradingViewCron;

	@Value("${external.fmz.cron:0 10 0/2 * * ?}")
	private String fmzCron;

	@Value("${external.github.cron:0 20 0/2 * * ?}")
	private String gitHubCron;

	@Value("${external.normalize.cron:0 0/30 * * * ?}")
	private String externalNormalizeCron;

	@Value("${external.dispatch.cron:0 15/30 * * * ?}")
	private String externalDispatchCron;

	@Value("${external.digest.cron:0 30 8 * * ?}")
	private String externalDigestCron;

	@Value("${strategy.system.report.enabled:true}")
	private boolean strategySystemReportEnabled;

	@Value("${strategy.system.report.cron:0 35 8 * * ?}")
	private String strategySystemReportCron;

	@Override
	public void run(String... args) throws Exception {
		com.gateway.connector.utils.Consts.WaitTime = 120 * 1000;
		if (dbPoolEnabled()) {
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
			log.info("DBPOOL enabled, bound database connection for default source:{}", dbPoolDefault);
		} else {
			log.info("DBPOOL disabled, skip binding database connection. clickHouseDefault:{}", clickHouseDefault);
		}
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
		logExternalSourceConfig();

		schedule.init(scheduleConfig);
		//runYesterdaysDataJob();

	
		schedule.start();
		registerExternalSourceJobs();
	}

	

//	private void runYesterdaysDataJob() {
//		log.info("yesterdaysDataEnabled:{} yesterdaysDataCron:{}", yesterdaysDataEnabled, yesterdaysDataCron);
//		if (yesterdaysDataEnabled) {
//			schedule.updateJob(yesterdaysDataCron, "YesterdaysDataJob", YesterdaysDataJob.class, this);
//		}
//	}

	public int runTradingViewDiscoverJob() {
		log.info("runTradingViewDiscoverJob start");
		int count = externalSourceFacade.discoverTradingView();
		log.info("runTradingViewDiscoverJob end, discovered:{}", count);
		return count;
	}

	public int runFmzDiscoverJob() {
		log.info("runFmzDiscoverJob start");
		int count = externalSourceFacade.discoverFmz();
		log.info("runFmzDiscoverJob end, discovered:{}", count);
		return count;
	}

	public int runGitHubDiscoverJob() {
		log.info("runGitHubDiscoverJob start");
		int count = externalSourceFacade.discoverGitHub();
		log.info("runGitHubDiscoverJob end, discovered:{}", count);
		return count;
	}

	public int runExternalNormalizeJob() {
		log.info("runExternalNormalizeJob start");
		int count = externalSourceFacade.normalizeReady();
		log.info("runExternalNormalizeJob end, processed:{}", count);
		return count;
	}

	public int runExternalDispatchJob() {
		log.info("runExternalDispatchJob start");
		int count = externalSourceFacade.dispatchReady();
		log.info("runExternalDispatchJob end, dispatched:{}", count);
		return count;
	}

	public String runExternalDigestJob() {
		log.info("runExternalDigestJob start");
		String path = externalSourceFacade.writeDigestReport();
		log.info("runExternalDigestJob end, report:{}", path);
		return path;
	}

	public String runStrategySystemDailyReportJob() {
		log.info("runStrategySystemDailyReportJob start");
		String path = strategySystemDailyReportService.generateDailyReport();
		log.info("runStrategySystemDailyReportJob end, report:{}", path);
		return path;
	}

	public String runStrategySystemDailyReportJob(String reportDate) {
		log.info("runStrategySystemDailyReportJob start, reportDate:{}", reportDate);
		String path = strategySystemDailyReportService.generateDailyReport(reportDate);
		log.info("runStrategySystemDailyReportJob end, reportDate:{}, report:{}", reportDate, path);
		return path;
	}

	private void registerExternalSourceJobs() {
		if (!externalSourceEnabled) {
			log.info("external source jobs skipped, enabled:false");
			return;
		}
		if (tradingViewEnabled) {
			schedule.updateJob(tradingViewCron, "tradingViewDiscoverJob", TradingViewDiscoverJob.class, this);
		}
		if (fmzEnabled) {
			schedule.updateJob(fmzCron, "fmzDiscoverJob", FmzDiscoverJob.class, this);
		}
		if (gitHubEnabled) {
			schedule.updateJob(gitHubCron, "gitHubDiscoverJob", GitHubDiscoverJob.class, this);
		}
		schedule.updateJob(externalNormalizeCron, "externalSourceNormalizeJob", ExternalSourceNormalizeJob.class, this);
		schedule.updateJob(externalDigestCron, "externalSourceDigestReportJob", ExternalSourceDigestReportJob.class, this);
		if (strategySystemReportEnabled) {
			schedule.updateJob(strategySystemReportCron, "strategySystemDailyReportJob", StrategySystemDailyReportJob.class, this);
		}
		if (externalSourceDispatchEnabled) {
			externalSourceFacade.initDispatchClientIfNeeded();
			schedule.updateJob(externalDispatchCron, "externalSourceDispatchJob", ExternalSourceDispatchJob.class, this);
		} else {
			log.info("external source dispatch job disabled by config");
		}
	}

	private void logExternalSourceConfig() {
		log.info("external source config enabled:{} dispatchEnabled:{} dbPoolDefault:{} clickHouseDefault:{} tradingViewEnabled:{} fmzEnabled:{} gitHubEnabled:{} tradingViewCron:{} fmzCron:{} gitHubCron:{} normalizeCron:{} dispatchCron:{} digestCron:{} systemReportEnabled:{} systemReportCron:{}",
				externalSourceEnabled, externalSourceDispatchEnabled, dbPoolDefault, clickHouseDefault, tradingViewEnabled, fmzEnabled,
				gitHubEnabled, tradingViewCron, fmzCron, gitHubCron, externalNormalizeCron, externalDispatchCron,
				externalDigestCron, strategySystemReportEnabled, strategySystemReportCron);
	}

	private boolean dbPoolEnabled() {
		return dbPoolDefault != null && !dbPoolDefault.trim().isEmpty() && DBUtils.dbpool != null;
	}

}
