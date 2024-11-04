package com.app.dc.service.job;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.app.dc.entity.*;
import com.app.dc.enums.ExceType;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.app.common.db.DBUtils;
import com.app.common.utils.JsonUtils;
import com.app.dc.data.Symbol;
import com.app.dc.po.SolanaCategoryDPrice;
import com.app.dc.po.SolanaTokenBalance;
import com.app.dc.service.BatchStart;
import com.app.dc.util.DataUtils;
import com.app.dc.utils.DateUtil;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PointsHourJob extends TranSQL implements Job {

	@Setter
	private BatchStart batchStart;

	private static boolean runFlag = false;

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		log.info("PointsHourJob execute");
		if (batchStart == null) {
			JobDataMap jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
			Object o = jobDataMap.get("Object");
			if (o instanceof Object[]) {
				Object[] objs = (Object[]) o;
				batchStart = (BatchStart) objs[0];
				log.info("batchStart:{}", batchStart);
			}

		}
		Job();
	}

	public synchronized void Job() {
		if (!runFlag) {
			runFlag = true;
			try {
				log.info("PointsHourJob Job start");
				String hour = getHourDate();
				List<String> lt = batchStart.dataFacade.getAllSpotTSymbolCategory();
				log.info("getAllSpotTSymbolCategory size:{},{}", lt.size(),lt);

				if (lt.size() != 0) {
					List<TPointsTradePositionHour> tradelist = getTPointsTradePositionHour(hour, lt);
					List<TPointsLpPositionHour> lpList = getTPointsLpPositionHour(hour, lt);
					List<TPointsProtocolPositionHour> protocolList = getTPointsProtocolPositionHour(tradelist, lpList,
							hour, lt);
					getTPointsUsersHour(tradelist, lpList, protocolList, hour);
				}
				log.info("PointsHourJob Job end");
			} catch (Exception e) {
				log.error("PointsHourJob error", e);
			} finally {
				runFlag = false;
			}
		} else {
			log.warn("PointsHourJob job is runing.");

		}
	}

	public static String getHourDate() throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH");
		String date = format.format(new Date());
		return date;
	}

	private List<TPointsProtocolPositionHour> getTPointsProtocolPositionHour(List<TPointsTradePositionHour> tradelist,
			List<TPointsLpPositionHour> lpList, String hour, List<String> lt) throws Exception {

		List<TPointsProtocolPositionHour> tSolanaHLpValues = new ArrayList<>();
		for (String symbolCategory : lt) {

			List<TPointsProtocolPositionHour> tSolanaHLpValues1 = getValut(symbolCategory);
			tSolanaHLpValues.addAll(tSolanaHLpValues1);

		}

		log.info("getTPointsProtocolPositionHour:{} size:{}", hour, tSolanaHLpValues.size());

		HashMap<String, BigDecimal> tradeHm = new HashMap<>();

		for (TPointsTradePositionHour tPointsTradePositionHour : tradelist) {
			String key = String.format("%s_%s", tPointsTradePositionHour.security_id,
					tPointsTradePositionHour.market_indicator);
			BigDecimal trade = tradeHm.get(key);
			if (trade == null) {
				trade = BigDecimal.ZERO;

			}
			if (tPointsTradePositionHour.token_amount != null) {
				trade = trade.add(tPointsTradePositionHour.token_amount);
			}
			tradeHm.put(key, trade);
		}
		HashMap<String, BigDecimal> lpHm = new HashMap<>();

		for (TPointsLpPositionHour tPointsTradePositionHour : lpList) {
			String key = String.format("%s_%s", tPointsTradePositionHour.security_id,
					tPointsTradePositionHour.market_indicator);
			BigDecimal lp = lpHm.get(key);
			if (lp == null) {
				lp = BigDecimal.ZERO;

			}
			if (tPointsTradePositionHour.token_amount != null) {
				lp = lp.add(tPointsTradePositionHour.token_amount);
			}
			lpHm.put(key, lp);
		}

		for (TPointsProtocolPositionHour tPointsTradePositionHour : tSolanaHLpValues) {
			tPointsTradePositionHour.trade_date = hour;
			Symbol symbol = batchStart.dataFacade.getSymbolAndExpiried(tPointsTradePositionHour.security_id);
			if (symbol != null) {
				tPointsTradePositionHour.currency = symbol.getSymbolLevel2Category();
			}
			String key = String.format("%s_%s", tPointsTradePositionHour.security_id,
					tPointsTradePositionHour.market_indicator);
			BigDecimal trade = tradeHm.get(key);
			BigDecimal lp = lpHm.get(key);

			tPointsTradePositionHour.trade_sum_amount = trade == null ? BigDecimal.ZERO : trade;
			tPointsTradePositionHour.lp_sum_amount = lp == null ? BigDecimal.ZERO : lp;

			tPointsTradePositionHour.token_amount = tPointsTradePositionHour.earn_amount
					.add(tPointsTradePositionHour.lp_amount).add(tPointsTradePositionHour.trade_amount)
					.subtract(tPointsTradePositionHour.trade_sum_amount)
					.subtract(tPointsTradePositionHour.lp_sum_amount);
			log.info("{}", tPointsTradePositionHour);
		}

		insertTPointsProtocolPositionHour(tSolanaHLpValues, hour);
		return tSolanaHLpValues;
	}

	private List<TPointsProtocolPositionHour> getValut(String symbolCategory) {
		String content = batchStart.tradeSvrClient.getValut("Trade" + symbolCategory + "Svr");
		ValutResult lpValueResult = JsonUtils.Deserialize(content, ValutResult.class);
		List<TPointsProtocolPositionHour> tSolanaHLpValues = new ArrayList<>();
		if (lpValueResult != null && lpValueResult.data != null) {
			String dateTime = DateUtil.getDefaultDate();
			for (SolanaTokenBalance lpValue : lpValueResult.data) {
				TPointsProtocolPositionHour tSolanaLpValue = new TPointsProtocolPositionHour();
				tSolanaLpValue.create_time = dateTime;
				tSolanaLpValue.security_id = lpValue.symbol;
				tSolanaLpValue.symbol = lpValue.symbol;
				tSolanaLpValue.user_id = lpValue.programId;
				tSolanaLpValue.market_indicator = symbolCategory;
				tSolanaLpValue.lp_amount = lpValue.lpTokenBalance == null ? BigDecimal.ZERO : lpValue.lpTokenBalance;
				tSolanaLpValue.trade_amount = lpValue.tradeTokenBalance == null ? BigDecimal.ZERO
						: lpValue.tradeTokenBalance;
				tSolanaLpValue.earn_amount = lpValue.earnTokenBalance == null ? BigDecimal.ZERO
						: lpValue.earnTokenBalance;
				tSolanaHLpValues.add(tSolanaLpValue);
			}
		}

		return tSolanaHLpValues;
	}

	public List<TPointsUsersHour> getTPointsUsersHour(List<TPointsTradePositionHour> tradelist,
			List<TPointsLpPositionHour> lpList, List<TPointsProtocolPositionHour> protocolList, String hour)
			throws Exception {
		HashMap<String, TPointsUsersHour> hm = new HashMap<>();
		String dateTime = DateUtil.getDefaultDate();

		List<TPointsUsersHour> list = new ArrayList<>();
		for (TPointsTradePositionHour tPointsLpPositionHour : tradelist) {
			String key = String.format("%s_%s", tPointsLpPositionHour.user_id, tPointsLpPositionHour.market_indicator);
			TPointsUsersHour ethena = hm.get(key);
			if (ethena == null) {
				ethena = new TPointsUsersHour();
				ethena.user_id = tPointsLpPositionHour.user_id;
				ethena.market_indicator = tPointsLpPositionHour.market_indicator;
				ethena.create_time = dateTime;
				ethena.trade_date = hour;
				hm.put(key, ethena);
			}
			if (tPointsLpPositionHour.token_amount != null) {
				ethena.token_amount = ethena.token_amount.add(tPointsLpPositionHour.token_amount);
			}
		}

		for (TPointsLpPositionHour tPointsLpPositionHour : lpList) {
			String key = String.format("%s_%s", tPointsLpPositionHour.user_id, tPointsLpPositionHour.market_indicator);
			TPointsUsersHour ethena = hm.get(key);
			if (ethena == null) {
				ethena = new TPointsUsersHour();
				ethena.user_id = tPointsLpPositionHour.user_id;
				ethena.market_indicator = tPointsLpPositionHour.market_indicator;
				ethena.create_time = dateTime;
				ethena.trade_date = hour;
				hm.put(key, ethena);
			}
			if (tPointsLpPositionHour.token_amount != null) {
				ethena.token_amount = ethena.token_amount.add(tPointsLpPositionHour.token_amount);
			}
		}
		for (TPointsProtocolPositionHour tPointsLpPositionHour : protocolList) {
			if (tPointsLpPositionHour.market_indicator.equals("USDE")) {
				tPointsLpPositionHour.user_id = "2koowWZUTSkdC9o2wgW12hpzEBj3S9JKmPy8WJbFZ4Zg";
			}
			String key = String.format("%s_%s", tPointsLpPositionHour.user_id, tPointsLpPositionHour.market_indicator);
			TPointsUsersHour ethena = hm.get(key);
			if (ethena == null) {
				ethena = new TPointsUsersHour();
				ethena.user_id = tPointsLpPositionHour.user_id;
				ethena.market_indicator = tPointsLpPositionHour.market_indicator;
				ethena.create_time = dateTime;
				ethena.trade_date = hour;
				hm.put(key, ethena);
			}
			if (tPointsLpPositionHour.token_amount != null) {
				ethena.token_amount = ethena.token_amount.add(tPointsLpPositionHour.token_amount);
			}
		}

		list.addAll(hm.values());
		log.info("getTPointsEthenaHour:{} size:{}", hour, list.size());

		insertTPointsEthenaHour(list, hour);
		return list;
	}

	public List<TPointsLpPositionHour> getTPointsLpPositionHour(String hour, List<String> lt) throws Exception {

		List<TPointsLpPositionHour> tSolanaHLpValues = new ArrayList<>();
		for (String symbolCategory : lt) {
			List<String> lps = getLp(symbolCategory);
			List<TPointsLpPositionHour> tSolanaHLpValues1 = getLpValue(symbolCategory, lps);
			tSolanaHLpValues.addAll(tSolanaHLpValues1);

		}

		log.info("getTPointsLpPositionHour:{} size:{}", hour, tSolanaHLpValues.size());

		for (TPointsLpPositionHour tPointsLpPositionHour : tSolanaHLpValues) {
			tPointsLpPositionHour.trade_date = hour;
			SolanaCategoryDPrice dprice = batchStart.epochFacade
					.getDPriceBySecurityId(tPointsLpPositionHour.security_id);
			Symbol symbol = batchStart.dataFacade.getSymbolAndExpiried(tPointsLpPositionHour.security_id);
			if (symbol != null) {
				tPointsLpPositionHour.currency = symbol.getSymbolLevel2Category();
			}
			if (dprice != null) {
				tPointsLpPositionHour.rate = dprice.rate_price;
				if (tPointsLpPositionHour.rate != null && tPointsLpPositionHour.rate.compareTo(BigDecimal.ZERO) != 0) {
					tPointsLpPositionHour.token_amount = tPointsLpPositionHour.token_a
							.add(tPointsLpPositionHour.token_b).add(tPointsLpPositionHour.reserve_base_amount)
							.add(tPointsLpPositionHour.reserve_quote_amount)
							.divide(tPointsLpPositionHour.rate, MathContext.DECIMAL128)
							.setScale(9, RoundingMode.HALF_EVEN);
				} else {
					log.warn("TPointsLpPositionHour {} dprice price:{}", tPointsLpPositionHour.security_id,
							dprice.rate_price);
				}
			} else {
				log.error("TPointsLpPositionHour {} dprice is not exist", tPointsLpPositionHour.security_id);
			}
		}

		insertTPointsLpPositionHour(tSolanaHLpValues, hour);
		return tSolanaHLpValues;
	}

	public List<String> getLp(String symbolCategory) {
		String sql = "SELECT lp FROM dc_solana_lp_users where is_active=1 and is_delete != '1' and market_indicator=? group by lp";
		log.info("getLp sql:{}", sql);
		List<String> lps = new ArrayList<>();
		List<TSolanaLpUsers> list = DBUtils.queryList(sql, new Object[] { symbolCategory }, TSolanaLpUsers.class);
		for (TSolanaLpUsers tSolanaLpUsers : list) {
			lps.add(tSolanaLpUsers.lp);
		}
		log.info("{} getLp size:{}", symbolCategory, lps.size());
		return lps;
	}

	public List<TPointsLpPositionHour> getLpValue(String symbolCategory, List<String> lps) {
		List<TPointsLpPositionHour> list = new ArrayList<>();
		List<String> req = new ArrayList<>();
		int count = 40;
		for (String lp : lps) {
			if (req.size() >= count) {
				List<TPointsLpPositionHour> list1 = getTradeLpValue(symbolCategory, req);
				list.addAll(list1);
				req.clear();
			} else {
				req.add(lp);
			}
		}
		if (req.size() > 0) {
			List<TPointsLpPositionHour> list1 = getTradeLpValue(symbolCategory, req);
			list.addAll(list1);
			req.clear();
		}

		return list;
	}

	private List<TPointsLpPositionHour> getTradeLpValue(String symbolCategory, List<String> req) {
		String content = batchStart.tradeSvrClient.getLPVAlue("Trade" + symbolCategory + "Svr", req);
		LPValueResult lpValueResult = JsonUtils.Deserialize(content, LPValueResult.class);
		List<TPointsLpPositionHour> tSolanaHLpValues = new ArrayList<>();
		if (lpValueResult != null && lpValueResult.data != null) {
			String dateTime = DateUtil.getDefaultDate();
			for (LPValue lpValue : lpValueResult.data) {
				TPointsLpPositionHour tSolanaLpValue = new TPointsLpPositionHour();
				tSolanaLpValue.create_time = dateTime;

				tSolanaLpValue.account_id = lpValue.lp;

				tSolanaLpValue.reserve_base_amount = lpValue.reserve_base_amount2;
				tSolanaLpValue.reserve_quote_amount = lpValue.reserve_quote_amount2;
				tSolanaLpValue.security_id = lpValue.securityId;
				tSolanaLpValue.symbol = lpValue.securityId;

				tSolanaLpValue.user_id = lpValue.user_id;
				tSolanaLpValue.token_a = lpValue.tokenA;
				tSolanaLpValue.token_b = lpValue.tokenB;

				tSolanaLpValue.market_indicator = symbolCategory;

				tSolanaHLpValues.add(tSolanaLpValue);
			}
		}
		return tSolanaHLpValues;
	}

	private List<TPointsTradePositionHour> getTPointsTradePositionHour(String hour, List<String> lt) throws Exception {
		String defaultDate = DateUtil.getDefaultDate();
		String marketIndicator=DataUtils.listToString(lt);
		String sql = "SELECT p.security_id,p.symbol,p.currency,p.user_id,p.account_id, p.market_indicator,CONVERT(p.inf1,decimal(35,9)) st,CONVERT(p.inf2,decimal(35,9)) as yt,b.balance FROM dc_orders_position p left join dc_users_balance b on p.account_id=b.account_id where p.islp='0' and p.account_id not like'%888888%' and (p.long_position!=0||p.short_position!=0) and p.market_indicator in("+marketIndicator+")";

		List<TPointsTradePositionHour> list = DBUtils.queryListThrowsException(sql, new Object[] {},
				TPointsTradePositionHour.class);
		log.info("getTPointsTradePositionHour:{} size:{}", hour, list.size());

		for (TPointsTradePositionHour tPointsTradePositionHour : list) {
			tPointsTradePositionHour.trade_date = hour;
			tPointsTradePositionHour.create_time = defaultDate;
			SolanaCategoryDPrice dprice = batchStart.epochFacade
					.getDPriceBySecurityId(tPointsTradePositionHour.security_id);
			if (dprice != null) {
				tPointsTradePositionHour.rate = dprice.rate_price;
				if (tPointsTradePositionHour.rate != null
						&& tPointsTradePositionHour.rate.compareTo(BigDecimal.ZERO) != 0) {
					tPointsTradePositionHour.token_amount = tPointsTradePositionHour.st.add(tPointsTradePositionHour.yt)
							.add(tPointsTradePositionHour.balance)
							.divide(tPointsTradePositionHour.rate, MathContext.DECIMAL128)
							.setScale(9, RoundingMode.HALF_EVEN);
				} else {
					log.warn("TPointsTradePositionHour {} dprice price:{}", tPointsTradePositionHour.security_id,
							dprice.rate_price);
				}
			} else {
				log.error("TPointsTradePositionHour {} dprice is not exist", tPointsTradePositionHour.security_id);
			}
		}

		insertTPointsTradePositionHour(list, hour);
		return list;
	}

	private void insertTPointsEthenaHour(List<TPointsUsersHour> tPointsEthenaHour, String hour) throws Exception {
		if (tPointsEthenaHour != null && tPointsEthenaHour.size() > 0) {
//			DBUtils.update("delete from dc_points_users_hour where trade_date=?", new Object[] { hour });
//			DBUtils.insertList(tPointsEthenaHour, "dc_points_users_hour");

			String sql = "delete from dc_points_users_hour where trade_date=?";
			ExecTablePo deExecTablePo = new ExecTablePo();
			deExecTablePo.sql = sql;
			deExecTablePo.args = new Object[] { hour };
			deExecTablePo.type = ExceType.delete;

			ExecTablePo<TPointsUsersHour> insert = new ExecTablePo();
			insert.tableName = "dc_points_users_hour";
			insert.insertlist = tPointsEthenaHour;
			insert.type = ExceType.insertList;

			List<ExecTablePo> execTablePoList = new ArrayList<>();
			execTablePoList.add(deExecTablePo);
			execTablePoList.add(insert);
			int num = tranSql(execTablePoList);
			log.info("insertTPointsEthenaHour size:{} tranSql number:{}", tPointsEthenaHour.size(), num);
		}
		log.info("insertTPointsEthenaHour:{} size:{}", hour, tPointsEthenaHour.size());
	}

	private void insertTPointsTradePositionHour(List<TPointsTradePositionHour> tPointsTradePositionHours, String hour)
			throws Exception {
		if (tPointsTradePositionHours != null && tPointsTradePositionHours.size() > 0) {
//			DBUtils.update("delete from dc_points_trade_position_hour where trade_date=?", new Object[] { hour });
//			DBUtils.insertList(tPointsTradePositionHours, "dc_points_trade_position_hour");

			String sql = "delete from dc_points_trade_position_hour where trade_date=?";
			ExecTablePo deExecTablePo = new ExecTablePo();
			deExecTablePo.sql = sql;
			deExecTablePo.args = new Object[] { hour };
			deExecTablePo.type = ExceType.delete;

			ExecTablePo<TPointsTradePositionHour> insert = new ExecTablePo();
			insert.tableName = "dc_points_trade_position_hour";
			insert.insertlist = tPointsTradePositionHours;
			insert.type = ExceType.insertList;

			List<ExecTablePo> execTablePoList = new ArrayList<>();
			execTablePoList.add(deExecTablePo);
			execTablePoList.add(insert);
			int num = tranSql(execTablePoList);
			log.info("insertTPointsTradePositionHour size:{} tranSql number:{}", tPointsTradePositionHours.size(), num);
		}
		log.info("insertTPointsTradePositionHour:{} size:{}", hour, tPointsTradePositionHours.size());
	}

	private void insertTPointsProtocolPositionHour(List<TPointsProtocolPositionHour> tPointsProtocolPositionHour,
			String hour) throws Exception {
		if (tPointsProtocolPositionHour != null && tPointsProtocolPositionHour.size() > 0) {
//			DBUtils.update("delete from dc_points_protocol_position_hour where trade_date=?", new Object[] { hour });
//			DBUtils.insertList(tPointsProtocolPositionHour, "dc_points_protocol_position_hour");

			String sql = "delete from dc_points_protocol_position_hour where trade_date=?";
			ExecTablePo deExecTablePo = new ExecTablePo();
			deExecTablePo.sql = sql;
			deExecTablePo.args = new Object[] { hour };
			deExecTablePo.type = ExceType.delete;

			ExecTablePo<TPointsProtocolPositionHour> insert = new ExecTablePo();
			insert.tableName = "dc_points_protocol_position_hour";
			insert.insertlist = tPointsProtocolPositionHour;
			insert.type = ExceType.insertList;

			List<ExecTablePo> execTablePoList = new ArrayList<>();
			execTablePoList.add(deExecTablePo);
			execTablePoList.add(insert);
			int num = tranSql(execTablePoList);
			log.info("insertTPointsProtocolPositionHour size:{} tranSql number:{}", tPointsProtocolPositionHour.size(),
					num);
		}
		log.info("insertTPointsProtocolPositionHour:{} size:{}", hour, tPointsProtocolPositionHour.size());
	}

	private void insertTPointsLpPositionHour(List<TPointsLpPositionHour> tPointsLpPositionHour, String hour)
			throws Exception {
		if (tPointsLpPositionHour != null && tPointsLpPositionHour.size() > 0) {
//			DBUtils.update("delete from dc_points_lp_position_hour where trade_date=?", new Object[] { hour });
//			DBUtils.insertList(tPointsLpPositionHour, "dc_points_lp_position_hour");

			String sql = "delete from dc_points_lp_position_hour where trade_date=?";
			ExecTablePo deExecTablePo = new ExecTablePo();
			deExecTablePo.sql = sql;
			deExecTablePo.args = new Object[] { hour };
			deExecTablePo.type = ExceType.delete;

			ExecTablePo<TPointsLpPositionHour> insert = new ExecTablePo();
			insert.tableName = "dc_points_lp_position_hour";
			insert.insertlist = tPointsLpPositionHour;
			insert.type = ExceType.insertList;

			List<ExecTablePo> execTablePoList = new ArrayList<>();
			execTablePoList.add(deExecTablePo);
			execTablePoList.add(insert);
			int num = tranSql(execTablePoList);
			log.info("insertTPointsLpPositionHour size:{} tranSql number:{}", tPointsLpPositionHour.size(), num);
		}
		log.info("insertTPointsLpPositionHour:{} size:{}", hour, tPointsLpPositionHour.size());
	}
}
