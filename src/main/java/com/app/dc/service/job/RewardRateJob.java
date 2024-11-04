package com.app.dc.service.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.app.common.db.DBUtils;
import com.app.common.utils.JsonUtils;
import com.app.common.utils.StringUtil;
import com.app.dc.data.Symbol;
import com.app.dc.engine.po.Position;
import com.app.dc.entity.*;
import com.app.dc.enums.ExceType;
import com.app.dc.fix.message.MarketDataSnapshotFullRefresh;
import com.app.dc.po.SolanaCategoryDPrice;
import com.app.dc.service.BatchStart;
import com.app.dc.util.Consts;
import com.app.dc.util.DataUtils;
import com.app.dc.utils.DateUtil;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class RewardRateJob extends TranSQL implements Job {

	private static boolean runFlag = false;

	@Setter
	private BatchStart batchStart;

	@Override
	public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
		log.info("RewardRateJob execute");
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
				log.info("RewardRateJob Job start");
				String startDate = getLastDate();
				String endDate = getDate();
//                String startDate = "2024-06-09 13";
//                String endDate = "2024-06-09 14";
				log.info("start date:{}", startDate);
				log.info("end date:{}", endDate);

				getSolanaHLpValue(startDate, endDate);
				calRewardRate(startDate, endDate);
				callPMobility(startDate, endDate);
				log.info("RewardRateJob Job end");
			} catch (Exception e) {
				log.error("RewardRateJob error", e);
			} finally {
				runFlag = false;
			}
		} else {
			log.warn("RewardRateJob job is runing.");

		}
	}

	public void getSolanaHLpValue(String startDate, String endDate) throws Exception {
		log.info("getLP start date:{} end date:{}", startDate, endDate);
		List<String> lt = batchStart.dataFacade.getAllTSymbolCategory();
		List<TSolanaHLpValue> tSolanaHLpValues = new ArrayList<>();
		for (String symbolCategory : lt) {
			List<String> lps = getLp(symbolCategory);
			List<TSolanaHLpValue> tSolanaHLpValues1 = getLpValue(symbolCategory,lps);
			tSolanaHLpValues.addAll(tSolanaHLpValues1);

		}
		updateTSolanaHLpValue(tSolanaHLpValues, endDate);
	}

	public void calRewardRate(String startDate, String endDate) throws Exception {
		log.info("calRewardRate start date:{} end date:{}", startDate, endDate);
		Map<String, TSolanaHFee> rewardRateMap = new HashMap<>();
		List<TSolanaHLpValue> sumLp = getSumLp(endDate);
		String create_time = DateUtil.getDefaultDate();

		for (TSolanaHLpValue tSolanaHLpValue : sumLp) {
			if (tSolanaHLpValue.lp_value != null) {
				TSolanaHFee rewardRate = rewardRateMap.get(tSolanaHLpValue.symbol);
				if (rewardRate == null) {
					rewardRate = new TSolanaHFee();
					rewardRate.create_time = create_time;
					rewardRateMap.put(tSolanaHLpValue.symbol, rewardRate);
					rewardRate.trade_date = endDate;
					rewardRate.symbol = tSolanaHLpValue.symbol;
					Symbol symbol = batchStart.dataFacade.getSymbol2(rewardRate.symbol);
					if (symbol == null) {
						log.warn("tSolanaHLpValue symbol is null. symbol:{} endDate:{}", rewardRate.symbol, endDate);
						continue;
					}
					rewardRate.market_indicator = symbol.getSymbolLevel1Category();
					SolanaCategoryDPrice solanaCategoryDPrice = batchStart.epochFacade
							.getDPriceBySecurityId(rewardRate.symbol);
					if (solanaCategoryDPrice == null) {
						log.warn("solanaCategoryDPrice is null. symbol:{} endDate:{}", rewardRate.symbol, endDate);
						continue;
					}
					rewardRate.market_rate = solanaCategoryDPrice.market_rate;
				}
				rewardRate.lp_value = tSolanaHLpValue.lp_value;
//				rewardRate.tvl = rewardRate.lp_value;
//				if (rewardRate.lp_value != null && rewardRate.market_rate != null) {
//					rewardRate.tvl = rewardRate.lp_value.multiply(rewardRate.market_rate).setScale(9,
//							RoundingMode.DOWN);
//				}
			}
		}

		List<MarketDataSnapshotFullRefresh> tradelist = batchStart.marketPriceFacade.getTrade();
		for (MarketDataSnapshotFullRefresh refresh : tradelist){
			if (StringUtil.isNotEmpty(refresh.getAvaLiquidity())){
				String securityId = refresh.getSecurityID();
				String marketIndicator = refresh.getMarketIndicator();
				TSolanaHFee rewardRate = rewardRateMap.get(securityId);

				if (rewardRate == null) {
					rewardRate = new TSolanaHFee();
					rewardRate.create_time = create_time;
					rewardRateMap.put(securityId, rewardRate);
					rewardRate.trade_date = endDate;
					rewardRate.symbol = securityId;
					rewardRate.market_indicator = marketIndicator;
				}
				BigDecimal tvl = null;

				try {
					tvl = new BigDecimal(refresh.getAvaLiquidity());
				}catch (Exception e){
					log.warn("MarketDataSnapshotFullRefresh tvl:{} convert BigDecimal error. symbol:{} endDate:{}", refresh.getAvaLiquidity(),rewardRate.symbol, endDate);
					continue;
				}

				rewardRate.tvl = tvl;
			}
		}
//        List<TOrdersExecorders> tOrdersExecordersFees = getTOrdersExecordersFee("2024-06-02 19",endDate);
		List<TOrdersExecorders> tOrdersExecordersFees = getTOrdersExecordersFee(startDate, endDate);

		for (TOrdersExecorders tOrdersExecorders : tOrdersExecordersFees) {
			TSolanaHFee rewardRate = rewardRateMap.get(tOrdersExecorders.symbol);
			if (rewardRate == null) {
				rewardRate = new TSolanaHFee();
				rewardRate.create_time = create_time;
				rewardRateMap.put(tOrdersExecorders.symbol, rewardRate);
				rewardRate.trade_date = endDate;
				rewardRate.symbol = tOrdersExecorders.symbol;
				rewardRateMap.put(rewardRate.symbol,rewardRate);
			}
			if (rewardRate.rate_price == null) {
				SolanaCategoryDPrice solanaCategoryDPrice = batchStart.epochFacade
						.getDPriceBySecurityId(tOrdersExecorders.symbol);
				if (solanaCategoryDPrice == null) {
					log.warn("TOrdersExecorders solanaCategoryDPrice is null. symbol:{} endDate:{}", rewardRate.symbol, endDate);
					continue;
				}
				rewardRate.rate_price = solanaCategoryDPrice.rate_price;
			}
			if (tOrdersExecorders.fee != null) {
				rewardRate.fee_a = tOrdersExecorders.fee;
				if (rewardRate.rate_price != null || rewardRate.rate_price.compareTo(BigDecimal.ZERO) !=0) {
					rewardRate.fee_b = tOrdersExecorders.fee.divide(rewardRate.rate_price,MathContext.DECIMAL128).setScale(9,RoundingMode.DOWN);
				}
			}
		}
		List<TSolanaHFee> tSolanaHFees = new ArrayList<>();
		tSolanaHFees.addAll(rewardRateMap.values());
		updatetTSolanaHFee(tSolanaHFees, endDate);
		List<String> rewardRateDay = Consts.RewardRateDay;
		List<TSolanaTermRewardRate> tSolanaTermRewardRates = new ArrayList<>();
		HashMap<String,TCategoryReferencePrice> categoryReferencePriceHm = getTCategoryReferencePrice(endDate);
		for (String days : rewardRateDay) {
			int day = Integer.parseInt(days);
			String startDay = getStartDate(day);
			create_time = DateUtil.getDefaultDate();
			log.info("calRewardRate start Day:{} end date day:{}", startDay, endDate);
			HashMap<String, TOrdersExecorders> stHm = getTOrdersExecordersSt(startDay, endDate);
			List<TSolanaHFeeEx> solanaHRewardRateExs = getTSolanaHFeeEx(startDay, endDate);
			int count = day * 24;

			for (TSolanaHFeeEx ex : solanaHRewardRateExs) {
				TSolanaTermRewardRate tSolanaTermRewardRate = new TSolanaTermRewardRate();
				tSolanaTermRewardRates.add(tSolanaTermRewardRate);
				tSolanaTermRewardRate.term = days + "D";
				String term = tSolanaTermRewardRate.term;
				if (days.equals("30")){
					term = "1M";
				}else if (days.equals("1")){
					term = "ON";
				}
				Symbol symbol1 = batchStart.dataFacade.getSymbolAndExpiried(ex.symbol);
				if (symbol1 == null) {
					log.warn("TCategoryReferencePrice symbol is null. symbol:{} endDate:{} term:{}", ex.symbol, endDate, tSolanaTermRewardRate.term);
				}else {
					 String key = symbol1.getSymbolLevel2Category()+term;
					 if (categoryReferencePriceHm.containsKey(key)) {
						 TCategoryReferencePrice referencePrice = categoryReferencePriceHm.get(key);
						 tSolanaTermRewardRate.reference_price = referencePrice.price;
					 }else {
						 log.warn("TCategoryReferencePrice is null. symbol:{} endDate:{} key:{}", ex.symbol, endDate, key);
					 }
				}

				tSolanaTermRewardRate.symbol = ex.symbol;
				tSolanaTermRewardRate.create_time = create_time;
				tSolanaTermRewardRate.market_indicator = ex.market_indicator;
				if (stHm.containsKey(ex.symbol)) {
					TOrdersExecorders st = stHm.get(ex.symbol);
					String info1 = st.info1;
					if (StringUtil.isNotEmpty(info1)) {
						try {
							tSolanaTermRewardRate.st_volume = new BigDecimal(info1);
						} catch (Exception e) {
							log.warn("symbol st error, symbol:{}, info1:{} term:{}", ex.symbol, info1, tSolanaTermRewardRate.term);
						}

					}
				}
				tSolanaTermRewardRate.trade_date = endDate;
				if (ex.tvl != null) {
					Symbol symbol = batchStart.dataFacade.getSymbol2(ex.symbol);
					if (symbol == null) {
						log.warn("tSolanaTermRewardRate symbol is null. symbol:{} endDate:{} term:{}", ex.symbol, endDate, tSolanaTermRewardRate.term);
						continue;
					}
					tSolanaTermRewardRate.market_indicator = symbol.getSymbolLevel1Category();
					BigDecimal rewardRate = null;
					BigDecimal tvl = ex.tvl;
					if (symbol.getSymbolLevel2Category().equals(Consts.JLP)||symbol.getSymbolLevel2Category().equals(Consts.SSOL)||symbol.getSymbolLevel2Category().equals(Consts.SUSD)||symbol.getSymbolLevel2Category().equals(Consts.BGSOL)){
						if (ex.fee_a == null){
							log.warn("tSolanaTermRewardRate ex.fee_a is null. symbol:{} endDate:{} term:{}", ex.symbol, endDate, tSolanaTermRewardRate.term);
							continue;
						}
						rewardRate = ex.fee_a.divide(tvl, MathContext.DECIMAL128).setScale(9, RoundingMode.DOWN);
					}else {
						if (ex.fee_b == null){
							log.warn("tSolanaTermRewardRate ex.fee_b is null. symbol:{} endDate:{} term:{}", ex.symbol, endDate , tSolanaTermRewardRate.term);
							continue;
						}
						rewardRate = ex.fee_b.divide(tvl, MathContext.DECIMAL128).setScale(9, RoundingMode.DOWN);
					}
					BigDecimal m = new BigDecimal(day+"");
					if (count > ex.count){
						m = new BigDecimal(String.valueOf(ex.count)).divide(new BigDecimal(String.valueOf(24)),MathContext.DECIMAL128).setScale(2,RoundingMode.DOWN);
						log.info("tSolanaTermRewardRate symbol:{} term:{} endDate:{} count:{} ex.count:{} m:{}",
								tSolanaTermRewardRate.symbol, tSolanaTermRewardRate.term,endDate, count, ex.count, m);
					}
					BigDecimal divideM = new BigDecimal("365").divide(m, MathContext.DECIMAL128)
							.setScale(9, RoundingMode.DOWN);
					BigDecimal apr = rewardRate.multiply(divideM).setScale(9, RoundingMode.DOWN);
					BigDecimal rewardRate1 = rewardRate.add(BigDecimal.ONE);
					double apy1 = Math.pow(rewardRate1.doubleValue(), divideM.doubleValue()) - 1;
					String apy1Str = DataUtils.getString(apy1);
					BigDecimal apy = new BigDecimal(apy1Str).setScale(9, RoundingMode.DOWN);
					log.info("tSolanaTermRewardRate symbol:{} term:{} endDate:{} count:{} ex.count:{} rewardRate:{} apr:{} apy:{} m:{} divideM:{}",
							tSolanaTermRewardRate.symbol, tSolanaTermRewardRate.term,endDate, count, ex.count, rewardRate, apr, apy,m,divideM);
					tSolanaTermRewardRate.reward_rate = rewardRate;
					tSolanaTermRewardRate.tvl = tvl;
					tSolanaTermRewardRate.apr = apr;
					tSolanaTermRewardRate.apy = apy;
				}else {
					log.warn("tSolanaTermRewardRate symbol:{} ex.tvl is null term:{} ex.count:{}",
							tSolanaTermRewardRate.symbol, tSolanaTermRewardRate.term, ex.count);
				}
			}
		}
		updateTSolanaTermRewardRate(tSolanaTermRewardRates, endDate);

	}

	public void callPMobility(String startDate, String endDate) throws Exception {
		log.info("callPMobility start date:{} end date:{}", startDate, endDate);
		List<TSolanaHLpValue> tSolanaHLpValues = getTSolanaHLpValue(endDate);
		Map<String, List<TSolanaHLpValue>> tSolanaHLpValuesHm = new HashMap<>();
		Map<String, LpSymbolMobility> lpSymbolMobilityHm = new HashMap<>();
		for (TSolanaHLpValue tSolanaHLpValue : tSolanaHLpValues) {
			List<TSolanaHLpValue> list = tSolanaHLpValuesHm.get(tSolanaHLpValue.symbol);
			if (list == null) {
				list = new ArrayList<>();
				tSolanaHLpValuesHm.put(tSolanaHLpValue.symbol, list);
			}
			list.add(tSolanaHLpValue);
			LpSymbolMobility lpSymbolMobility = lpSymbolMobilityHm.get(tSolanaHLpValue.symbol);
			if (lpSymbolMobility == null) {
				lpSymbolMobility = new LpSymbolMobility();
				lpSymbolMobility.symbol = tSolanaHLpValue.symbol;
				lpSymbolMobility.minLimit = tSolanaHLpValue.lower_rate2;
				lpSymbolMobility.maxLimit = tSolanaHLpValue.upper_rate2;
				lpSymbolMobilityHm.put(tSolanaHLpValue.symbol, lpSymbolMobility);
			}
			if (lpSymbolMobility.minLimit.compareTo(tSolanaHLpValue.lower_rate2) > 0) {
				lpSymbolMobility.minLimit = tSolanaHLpValue.lower_rate2;
			}
			if (lpSymbolMobility.maxLimit.compareTo(tSolanaHLpValue.upper_rate2) < 0) {
				lpSymbolMobility.maxLimit = tSolanaHLpValue.upper_rate2;
			}
		}

		Set<Map.Entry<String, LpSymbolMobility>> lpSymbolMobilitySet = lpSymbolMobilityHm.entrySet();
		log.info("MobilitySplit:{}", batchStart.getMobilitySplit());
		BigDecimal mobilitySplit = new BigDecimal(batchStart.getMobilitySplit());
		Map<String, List<LpSymbolMobilityLimit>> limitsHm = new HashMap<>();
		for (Map.Entry<String, LpSymbolMobility> mobilityEntry : lpSymbolMobilitySet) {
			LpSymbolMobility lpSymbolMobility = mobilityEntry.getValue();
			log.info("lpSymbolMobility:{}", lpSymbolMobility);
			List<LpSymbolMobilityLimit> limits = new ArrayList<>();
			BigDecimal count = BigDecimal.ZERO;
			calLimits(lpSymbolMobility, lpSymbolMobility.minLimit, mobilitySplit, limits, count);
			String limitsStr = JsonUtils.Serializer(limits);
			log.info("calLimits limitsStr:{}", limitsStr);
			limitsHm.put(mobilityEntry.getKey(), limits);
		}

		Map<String, TSolanaSymbolLpMobility> mobilityHm = new HashMap<>();
		String create_time = DateUtil.getDefaultDate();
		for (TSolanaHLpValue tSolanaHLpValue : tSolanaHLpValues) {
			List<LpSymbolMobilityLimit> limits = limitsHm.get(tSolanaHLpValue.symbol);
//            List<LpSymbolMobilityLimit> coverageLimits = getLPCoverageLimits(tSolanaHLpValue,limits);
			for (LpSymbolMobilityLimit limit : limits) {
				boolean status = checkLPCoverageLimits(tSolanaHLpValue, limit);
				String key = tSolanaHLpValue.symbol + "_" + limit.lower_limit.toPlainString() + "_"
						+ limit.upper_limit.toPlainString();
				TSolanaSymbolLpMobility mobility = mobilityHm.get(key);
				if (mobility == null) {
					mobility = new TSolanaSymbolLpMobility();
					mobility.symbol = tSolanaHLpValue.symbol;
					mobility.lower_limit = limit.lower_limit;
					mobility.upper_limit = limit.upper_limit;
					if (status) {
						mobility.mobility = tSolanaHLpValue.lp_value;
					} else {
						mobility.mobility = BigDecimal.ZERO;
					}
					mobility.trade_date = endDate;
					mobility.create_time = create_time;
					mobilityHm.put(key, mobility);
				} else {
					if (status) {
						if (tSolanaHLpValue.lp_value !=null) {
							if (mobility.mobility == null) {
								mobility.mobility = tSolanaHLpValue.lp_value;
							} else {
								mobility.mobility = mobility.mobility.add(tSolanaHLpValue.lp_value);
							}
						}
					}
				}
			}

		}
		List<TSolanaSymbolLpMobility> tSolanaSymbolLpMobilityList = new ArrayList<>();
		tSolanaSymbolLpMobilityList.addAll(mobilityHm.values());

		solanaSymbolLpMobilityAsc(tSolanaSymbolLpMobilityList);
		List<TSolanaSymbolLpMobility> removeLpMobility = new ArrayList<>();
		for (TSolanaSymbolLpMobility po : tSolanaSymbolLpMobilityList){
			if (po.mobility == null || po.mobility.compareTo(BigDecimal.ZERO) == 0){
				removeLpMobility.add(po);
			}else {
				break;
			}
		}

		solanaSymbolLpMobilityDesc(tSolanaSymbolLpMobilityList);
		for (TSolanaSymbolLpMobility po : tSolanaSymbolLpMobilityList){
			if (po.mobility == null || po.mobility.compareTo(BigDecimal.ZERO) == 0){
				removeLpMobility.add(po);
			}else {
				break;
			}
		}
		for (TSolanaSymbolLpMobility po : removeLpMobility){
			tSolanaSymbolLpMobilityList.remove(po);
		}

		solanaSymbolLpMobilityAsc(tSolanaSymbolLpMobilityList);
		updateTSolanaSymbolLpMobility(tSolanaSymbolLpMobilityList, endDate);
	}

	public void calLimits(LpSymbolMobility lpSymbolMobility, BigDecimal lastLimit, BigDecimal mobilitySplit,
			List<LpSymbolMobilityLimit> limits,BigDecimal count) {
		BigDecimal next = lastLimit.add(mobilitySplit);
		log.info("calLimits lastLimit:{} next:{} lpSymbolMobility.maxLimit:{}",lastLimit,next,lpSymbolMobility.maxLimit);
		if (next.compareTo(lpSymbolMobility.maxLimit) > 0) {
			return;
		}
		if (count.compareTo(batchStart.getRecursionCount()) >0){
			log.info("calLimits count:{} recursionCount:{}",count,batchStart.getRecursionCount());
			return;
		}
		count = count.add(BigDecimal.ONE);
		LpSymbolMobilityLimit lpSymbolMobilityLimit = new LpSymbolMobilityLimit();
		lpSymbolMobilityLimit.lower_limit = lastLimit;
		lpSymbolMobilityLimit.upper_limit = next;
		limits.add(lpSymbolMobilityLimit);
		calLimits(lpSymbolMobility, next, mobilitySplit, limits,count);
	}

	public List<LpSymbolMobilityLimit> getLPCoverageLimits(TSolanaHLpValue tSolanaHLpValue,
			List<LpSymbolMobilityLimit> limits) {
		List<LpSymbolMobilityLimit> lpSymbolMobilityLimits = new ArrayList<>();
		for (LpSymbolMobilityLimit limit : limits) {
			if (tSolanaHLpValue.lower_rate2.compareTo(limit.lower_limit) >= 0
					&& tSolanaHLpValue.lower_rate2.compareTo(limit.upper_limit) < 0) {
				lpSymbolMobilityLimits.add(limit);
			} else if (limit.lower_limit.compareTo(tSolanaHLpValue.lower_rate2) >= 0
					&& tSolanaHLpValue.upper_rate2.compareTo(limit.lower_limit) > 0) {
				lpSymbolMobilityLimits.add(limit);
			}
		}
		return lpSymbolMobilityLimits;
	}

	private boolean checkLPCoverageLimits(TSolanaHLpValue tSolanaHLpValue, LpSymbolMobilityLimit limit) {
		boolean status = false;
		if (tSolanaHLpValue.lower_rate2.compareTo(limit.lower_limit) >= 0
				&& tSolanaHLpValue.lower_rate2.compareTo(limit.upper_limit) < 0) {
			status = true;
		} else if (limit.lower_limit.compareTo(tSolanaHLpValue.lower_rate2) >= 0
				&& tSolanaHLpValue.upper_rate2.compareTo(limit.lower_limit) > 0) {
			status = true;
		}
		return status;
	}

	public static String getDate() throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH");
		String date = format.format(new Date());
		return date;
	}

	public static String getLastDate() throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH");
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.HOUR_OF_DAY, -1);
		String date = format.format(cal.getTime());
		return date;
	}

	public static String getStartDate(int day) throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH");
		Calendar cal = Calendar.getInstance();
		day = day * -1;
		cal.add(Calendar.DAY_OF_YEAR, day);
		String date = format.format(cal.getTime());
		return date;
	}

	public List<TOrdersExecorders> getTOrdersExecordersFee(String startDate, String endDate) {
		String sql = "select symbol,sum(fee) as fee from dc_orders_execorders where transact_time >= ? and transact_time <= ? group by symbol";
		log.info("getTOrdersExecorders sql:{}", sql);
		List<TOrdersExecorders> list = DBUtils.queryList(sql, new Object[] { startDate, endDate },
				TOrdersExecorders.class);
		log.info("getTOrdersExecorders size:{}", list.size());
		return list;
	}

	public HashMap<String, TOrdersExecorders> getTOrdersExecordersSt(String startDate, String endDate) {
		String sql = "select symbol,CONCAT('',sum(abs(info1))) as info1 from dc_orders_execorders where transact_time > ? and transact_time <= ? group by symbol";
		log.info("getTOrdersExecordersSt sql:{}", sql);
		List<TOrdersExecorders> list = DBUtils.queryList(sql, new Object[] { startDate, endDate },
				TOrdersExecorders.class);
		HashMap<String, TOrdersExecorders> hm = new HashMap<>();
		for (TOrdersExecorders tOrdersExecorders : list) {
			hm.put(tOrdersExecorders.symbol, tOrdersExecorders);
		}
		log.info("getTOrdersExecordersSt size:{} hm size:{}", list.size(), hm.size());
		return hm;
	}

	public List<TSolanaHLpValue> getSumLp(String endDate) {
		String sql = "SELECT symbol,sum(lp_value) as lp_value FROM dc_solana_h_lp_value where trade_date=? group by symbol";
		log.info("getSumLp sql:{}", sql);
		List<TSolanaHLpValue> list = DBUtils.queryList(sql, new Object[] { endDate }, TSolanaHLpValue.class);
		log.info("getSumLp size:{}", list.size());
		return list;
	}

	public List<String> getLp(String symbolCategory) {
		String sql = "SELECT lp FROM dc_solana_lp_users where is_active=1 and is_delete != '1' and market_indicator=? group by lp";
		log.info("getLp sql:{}", sql);
		List<String> lps = new ArrayList<>();
		List<TSolanaLpUsers> list = DBUtils.queryList(sql,  new Object[] { symbolCategory }, TSolanaLpUsers.class);
		for (TSolanaLpUsers tSolanaLpUsers : list) {
			lps.add(tSolanaLpUsers.lp);
		}
		log.info("{} getLp size:{}", symbolCategory,lps.size());
		return lps;
	}

	public List<TSolanaHLpValue> getLpValue(String symbolCategory,List<String> lps) {
		List<TSolanaHLpValue> list = new ArrayList<>();
		List<String> req = new ArrayList<>();
		int count = 40;
		for (String lp : lps) {
			if (req.size() >= count) {
				List<TSolanaHLpValue> list1 = getTradeLpValue(symbolCategory,req);
				list.addAll(list1);
				req.clear();
			} else {
				req.add(lp);
			}
		}
		if (req.size() > 0) {
			List<TSolanaHLpValue> list1 = getTradeLpValue(symbolCategory,req);
			list.addAll(list1);
			req.clear();
		}

		return list;
	}

	private List<TSolanaHLpValue> getTradeLpValue(String symbolCategory,List<String> req) {
		String content = batchStart.tradeSvrClient.getLPVAlue("Trade"+symbolCategory+"Svr",req);
		LPValueResult lpValueResult = JsonUtils.Deserialize(content, LPValueResult.class);
		List<TSolanaHLpValue> tSolanaHLpValues = new ArrayList<>();
		if (lpValueResult != null && lpValueResult.data != null) {
			String dateTime = DateUtil.getDefaultDate();
			for (LPValue lpValue : lpValueResult.data) {
				TSolanaHLpValue tSolanaLpValue = new TSolanaHLpValue();
				tSolanaLpValue.create_time = dateTime;
				tSolanaLpValue.liquidity = new BigDecimal(String.valueOf(lpValue.liquidity));
				tSolanaLpValue.lower_rate = new BigDecimal(String.valueOf(lpValue.lower_rate));
				tSolanaLpValue.lp = lpValue.lp;
				tSolanaLpValue.lp_value = lpValue.lpvalue;
				tSolanaLpValue.market_index = lpValue.marketIndex;
				tSolanaLpValue.perp_market = lpValue.perpMarket;
				tSolanaLpValue.reserve_base_amount = new BigDecimal(String.valueOf(lpValue.reserve_base_amount));
				tSolanaLpValue.reserve_quote_amount = new BigDecimal(String.valueOf(lpValue.reserve_quote_amount));
				tSolanaLpValue.symbol = lpValue.securityId;
				tSolanaLpValue.tick_lower_index = new BigDecimal(String.valueOf(lpValue.tick_lower_index));
				tSolanaLpValue.tick_upper_index = new BigDecimal(String.valueOf(lpValue.tick_upper_index));
				tSolanaLpValue.upper_rate = new BigDecimal(String.valueOf(lpValue.upper_rate));
				tSolanaLpValue.user_id = lpValue.user_id;
				tSolanaLpValue.token_a = lpValue.tokenA;
				tSolanaLpValue.token_b = lpValue.tokenB;
				tSolanaLpValue.liquidity2 = lpValue.liquidity2;
				tSolanaLpValue.lower_rate2 = lpValue.lower_rate2;
				tSolanaLpValue.upper_rate2 = lpValue.upper_rate2;
				tSolanaLpValue.reserve_base_amount2 = lpValue.reserve_base_amount2;
				tSolanaLpValue.reserve_quote_amount2 = lpValue.reserve_quote_amount2;
				tSolanaLpValue.sqrt_price = new BigDecimal(String.valueOf(lpValue.sqrtPrice));
				tSolanaLpValue.sqrt_price2 = lpValue.sqrtPrice2;
				tSolanaLpValue.market_indicator = symbolCategory;
				tSolanaHLpValues.add(tSolanaLpValue);
			}
		}
		return tSolanaHLpValues;
	}

	public static String getDefaultDate() {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return simpleDateFormat.format(new Date());
	}

	private void updateTSolanaHLpValue(List<TSolanaHLpValue> tSolanaHLpValues, String endDate) throws Exception {
//		DBUtils.update("delete from dc_solana_h_lp_value where trade_date=?", new Object[] { endDate });
		for (TSolanaHLpValue tSolanaHLpValue : tSolanaHLpValues) {
			tSolanaHLpValue.trade_date = endDate;
		}
//		DBUtils.insertList(tSolanaHLpValues, "dc_solana_h_lp_value");

		String sql = "delete from dc_solana_h_lp_value where trade_date=?";
		ExecTablePo deExecTablePo = new ExecTablePo();
		deExecTablePo.sql = sql;
		deExecTablePo.args = new Object[]{endDate};
		deExecTablePo.type = ExceType.delete;

		ExecTablePo<TSolanaHLpValue> insert = new ExecTablePo();
		insert.tableName = "dc_solana_h_lp_value";
		insert.insertlist = tSolanaHLpValues;
		insert.type = ExceType.insertList;

		List<ExecTablePo> execTablePoList = new ArrayList<>();
		execTablePoList.add(deExecTablePo);
		execTablePoList.add(insert);
		int num = tranSql(execTablePoList);
		log.info("updateTSolanaHLpValue trade_date:{} size:{} number:{}", endDate, tSolanaHLpValues.size(), num);
	}

	private void updatetTSolanaHFee(List<TSolanaHFee> tSolanaHFees, String endDate) throws Exception {
//		DBUtils.update("delete from dc_solana_h_fee where trade_date=?", new Object[] { endDate });
//		DBUtils.insertList(tSolanaHFees, "dc_solana_h_fee");


		String sql = "delete from dc_solana_h_fee where trade_date=?";
		ExecTablePo deExecTablePo = new ExecTablePo();
		deExecTablePo.sql = sql;
		deExecTablePo.args = new Object[]{endDate};
		deExecTablePo.type = ExceType.delete;

		ExecTablePo<TSolanaHFee> insert = new ExecTablePo();
		insert.tableName = "dc_solana_h_fee";
		insert.insertlist = tSolanaHFees;
		insert.type = ExceType.insertList;

		List<ExecTablePo> execTablePoList = new ArrayList<>();
		execTablePoList.add(deExecTablePo);
		execTablePoList.add(insert);
		int num = tranSql(execTablePoList);

		log.info("updatetTSolanaHFee tradeDate:{} size:{} number:{}", endDate, tSolanaHFees.size(),num);
	}

	public List<TSolanaHFeeEx> getTSolanaHFeeEx(String startDayDate, String endDayDate) {
		String sql = "SELECT symbol,avg(tvl) as tvl,sum(fee_a) as fee_a,sum(fee_b) as fee_b,count(trade_date) as count FROM dc_solana_h_fee where trade_date > ? and trade_date <= ?  group by symbol";
		log.info("getTSolanaHFeeEx sql:{} startDayDate:{} endDayDate:{}", sql, startDayDate, endDayDate);
		List<TSolanaHFeeEx> list = DBUtils.queryList(sql, new Object[] { startDayDate, endDayDate },
				TSolanaHFeeEx.class);
		log.info("getTSolanaHFeeEx size:{}", list.size());
		return list;
	}

	public HashMap<String,TCategoryReferencePrice> getTCategoryReferencePrice(String endDate) {
		if (endDate.length() >10){
			endDate = endDate.substring(0,10);
		}
		String sql = "select p.* from dc_category_reference_price p right join (SELECT max(create_time) as create_time,symbol_category FROM dc_category_reference_price where left(create_time,10) <= ? group by symbol_category) r on p.create_time=r.create_time and p.symbol_category=r.symbol_category";
		log.info("getTCategoryReferencePrice sql:{}", sql);
		List<TCategoryReferencePrice> list = DBUtils.queryList(sql, new Object[] {endDate },TCategoryReferencePrice.class);
		log.info("getTCategoryReferencePrice list size:{}", list.size());
		HashMap<String,TCategoryReferencePrice> hm = new HashMap<>();
		for (TCategoryReferencePrice price : list){
			String key = price.symbol_category+price.term;
			hm.put(key,price);
		}
		log.info("getTCategoryReferencePrice hm size:{}", hm.size());
		return hm;
	}

	private void updateTSolanaTermRewardRate(List<TSolanaTermRewardRate> tSolanaTermRewardRates, String endDate)
			throws Exception {
//		DBUtils.update("delete from dc_solana_term_reward_rate where trade_date=?", new Object[] { endDate });
//		DBUtils.insertList(tSolanaTermRewardRates, "dc_solana_term_reward_rate");
//		log.info("updateTSolanaTermRewardRate tradeDate:{} size:{}", endDate, tSolanaTermRewardRates.size());


		String sql = "delete from dc_solana_term_reward_rate where trade_date=?";
		ExecTablePo deExecTablePo = new ExecTablePo();
		deExecTablePo.sql = sql;
		deExecTablePo.args = new Object[]{endDate};
		deExecTablePo.type = ExceType.delete;

		ExecTablePo<TSolanaTermRewardRate> insert = new ExecTablePo();
		insert.tableName = "dc_solana_term_reward_rate";
		insert.insertlist = tSolanaTermRewardRates;
		insert.type = ExceType.insertList;

		List<ExecTablePo> execTablePoList = new ArrayList<>();
		execTablePoList.add(deExecTablePo);
		execTablePoList.add(insert);
		int num = tranSql(execTablePoList);

		log.info("updateTSolanaTermRewardRate tradeDate:{} size:{} number:{}", endDate, tSolanaTermRewardRates.size(),num);
	}

	public List<TSolanaHLpValue> getTSolanaHLpValue(String endDate) {
		String sql = "SELECT * FROM dc_solana_h_lp_value where trade_date=?";
		log.info("getTSolanaHLpValue sql:{}", sql);
		List<TSolanaHLpValue> list = DBUtils.queryList(sql, new Object[] { endDate }, TSolanaHLpValue.class);
		log.info("getTSolanaHLpValue size:{}", list.size());
		return list;
	}

	private void updateTSolanaSymbolLpMobility(List<TSolanaSymbolLpMobility> tSolanaSymbolLpMobilityList,
			String endDate) throws Exception {
//		DBUtils.update("delete from dc_solana_symbol_lp_mobility where trade_date=?", new Object[] { endDate });
//		DBUtils.update("delete from dc_solana_symbol_lp_mobility", new Object[] {});
//		DBUtils.insertList(tSolanaSymbolLpMobilityList, "dc_solana_symbol_lp_mobility");
//		log.info("updateTSolanaSymbolLpMobility tradeDate:{} size:{}", endDate, tSolanaSymbolLpMobilityList.size());


		String sql = "delete from dc_solana_symbol_lp_mobility";
		ExecTablePo deExecTablePo = new ExecTablePo();
		deExecTablePo.sql = sql;
//		deExecTablePo.args = new Object[]{endDate};
		deExecTablePo.args = new Object[]{};
		deExecTablePo.type = ExceType.delete;

		ExecTablePo<TSolanaSymbolLpMobility> insert = new ExecTablePo();
		insert.tableName = "dc_solana_symbol_lp_mobility";
		insert.insertlist = tSolanaSymbolLpMobilityList;
		insert.type = ExceType.insertList;

		List<ExecTablePo> execTablePoList = new ArrayList<>();
		execTablePoList.add(deExecTablePo);
		execTablePoList.add(insert);
		int num = tranSql(execTablePoList);

		log.info("updateTSolanaSymbolLpMobility tradeDate:{} size:{} number:{}", endDate, tSolanaSymbolLpMobilityList.size(),num);
	}

	private void solanaSymbolLpMobilityAsc(List<TSolanaSymbolLpMobility> list){
		Collections.sort(list, Comparator.comparing(TSolanaSymbolLpMobility::getLower_limit, Comparator.nullsFirst(Comparator.naturalOrder())));
	}
	private void solanaSymbolLpMobilityDesc(List<TSolanaSymbolLpMobility> list){
		Collections.sort(list, Comparator.comparing(TSolanaSymbolLpMobility::getLower_limit, Comparator.nullsFirst(Comparator.naturalOrder())).reversed());
	}
}
