package com.app.dc.service.job;

import com.app.dc.entity.ExecTablePo;
import com.app.dc.entity.TSymbolLpInterval;
import com.app.dc.enums.ExceType;
import com.app.dc.price.MarketPrice;
import com.app.dc.service.BatchStart;
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
import java.util.*;

@Slf4j
public class LPIntervalJob extends TranSQL implements Job {

    @Setter
    private BatchStart batchStart;

    private static boolean runFlag = false;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        log.info("LPIntervalJob execute");
        if (batchStart == null) {
            JobDataMap jobDataMap = jobExecutionContext.getJobDetail().getJobDataMap();
            Object o = jobDataMap.get("Object");
            if (o instanceof Object[]){
                Object[] objs = (Object[])o;
                batchStart = (BatchStart)objs[0];
                log.info("batchStart:{}",batchStart);
            }

        }
        Job();
    }

    public synchronized void Job() {
        if (!runFlag) {
            runFlag = true;
            try {
                log.info("LPIntervalJob Job start");
                calLPInterval();
                log.info("LPIntervalJob Job end");
            } catch (Exception e) {
                log.error("LPIntervalJob error", e);
            }finally {
                runFlag = false;
            }
        } else {
            log.warn("LPIntervalJob job is runing.");

        }
    }

    public void calLPInterval() throws Exception {
        log.info("LPInterval:{} LPIntervalMultiple:{}",batchStart.getLPInterval(),batchStart.getLPIntervalMultiple());
        BigDecimal interval = new BigDecimal(batchStart.getLPInterval());
        List<String> LPIntervalMultiples = DataUtils.getListStr(batchStart.getLPIntervalMultiple());
        HashMap<String, MarketPrice> marketPriceHashMap = batchStart.marketPriceFacade.getMarketPriceMap();
        String defaultDate = DateUtil.getDefaultDate();
        BigDecimal percentage = new BigDecimal("100");
        Set<Map.Entry<String, MarketPrice>> entrySet = marketPriceHashMap.entrySet();
        List<String> keys = new ArrayList<>();
        List<TSymbolLpInterval> lpIntervalList = new ArrayList<>();
        for (Map.Entry<String, MarketPrice> entry : entrySet) {
            MarketPrice marketPrice = entry.getValue();
            log.info(marketPrice.toString());
            BigDecimal yield = marketPrice.getYield();
            if (yield== null){
                continue;
            }
            BigDecimal yield100 = yield.multiply(percentage);
            BigDecimal interval100 = interval.multiply(percentage);
            BigDecimal divideYield = yield100.divide(interval100, MathContext.DECIMAL128);
            BigDecimal lower = divideYield.setScale(0, RoundingMode.FLOOR).multiply(interval100);
            BigDecimal upper = divideYield.setScale(0, RoundingMode.CEILING).multiply(interval100);
            log.info("securityId:{} lower:{} upper:{}",marketPrice.getSecurityId(),lower,upper);
            for (String recommendIntervals : LPIntervalMultiples){
                TSymbolLpInterval tSymbolLpInterval = new TSymbolLpInterval();
                BigDecimal b = new BigDecimal(recommendIntervals);
                BigDecimal recommendLower = lower.divide(b, MathContext.DECIMAL128).divide(interval100, MathContext.DECIMAL128).setScale(0, RoundingMode.FLOOR).multiply(interval100);
                BigDecimal recommendUpper = upper.add(lower.subtract(recommendLower));
                tSymbolLpInterval.symbol = marketPrice.getSecurityId();
                tSymbolLpInterval.multiples = b;
                tSymbolLpInterval.yield = yield;
                tSymbolLpInterval.lower = recommendLower.divide(percentage);
                tSymbolLpInterval.upper = recommendUpper.divide(percentage);
                tSymbolLpInterval.create_time = defaultDate;
                log.info(tSymbolLpInterval.toString());
                String key = tSymbolLpInterval.symbol+"_"+tSymbolLpInterval.multiples;
                if (!keys.contains(key)) {
                    keys.add(key);
                    lpIntervalList.add(tSymbolLpInterval);
                }
            }

        }
        insertTSymbolLpInterval(lpIntervalList);

    }


    private void insertTSymbolLpInterval(List<TSymbolLpInterval> tSymbolLpIntervals) throws Exception {
        if (tSymbolLpIntervals != null && tSymbolLpIntervals.size() > 0) {
//            DBUtils.update("delete from dc_symbol_lp_interval",new Object[]{});
//            DBUtils.insertList(tSymbolLpIntervals, "dc_symbol_lp_interval");


            String sql = "delete from dc_symbol_lp_interval";
            ExecTablePo deExecTablePo = new ExecTablePo();
            deExecTablePo.sql = sql;
            deExecTablePo.args = new Object[] { };
            deExecTablePo.type = ExceType.delete;

            ExecTablePo<TSymbolLpInterval> insert = new ExecTablePo();
            insert.tableName ="dc_symbol_lp_interval";
            insert.insertlist = tSymbolLpIntervals;
            insert.type = ExceType.insertList;

            List<ExecTablePo> execTablePoList = new ArrayList<>();
            execTablePoList.add(deExecTablePo);
            execTablePoList.add(insert);
            int num = tranSql(execTablePoList);
            log.info("insertTSymbolLpInterval size:{} tranSql number:{}", tSymbolLpIntervals.size(),num);
        }
    }
}
