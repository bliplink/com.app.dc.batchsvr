package com.app.dc.service.job;

import com.app.common.db.DBUtils;
import com.app.dc.entity.*;
import com.app.dc.enums.ExceType;
import com.app.dc.enums.ToChainStatus;
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
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class PointsDayJob extends TranSQL implements Job {

    @Setter
    private BatchStart batchStart;

    private static boolean runFlag = false;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        log.info("PointsDayJob execute");
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
                log.info("PointsDayJob Job start");
                String date = getDate();
                log.info("PointsDayJob date:{}",date);
                List<TPointsLpPositionDay> lpPositionDays = getTPointsLpPositionDayByHour(date);
                insertTPointsLpPositionDay(lpPositionDays,date);
                List<TPointsProtocolPositionDay> protocolPositionDays = getTPointsProtocolPositionDayByHour(date);
                insertTPointsProtocolPositionDay(protocolPositionDays,date);
                List<TPointsTradePositionDay> tradePositionDays = getTPointsTradePositionDayByHour(date);
                insertTPointsTradePositionDay(tradePositionDays,date);
                calSSolPoints(date);
                log.info("PointsDayJob Job end");
            } catch (Exception e) {
                log.error("PointsDayJob error", e);
            }finally {
                runFlag = false;
            }
        } else {
            log.warn("PointsDayJob job is runing.");

        }
    }

    private void calSSolPoints(String tradeDate) throws Exception {
        HashMap<String, BigDecimal> sumProtocolTokenAmount = getSumProtocolTokenAmount(tradeDate);
        HashMap<String, BigDecimal> sumLpTokenAmount = getSumLPTokenAmount(tradeDate);
        HashMap<String, BigDecimal> sumTradeTokenAmount = getSumTradeTokenAmount(tradeDate);
        List<TPointsProtocolPositionDay> protocolPositionDays = getTPointsProtocolPositionDayBySSol(tradeDate);
        List<TPointsLpPositionDay> lpPositionDays = getTPointsLpPositionDayBySSol(tradeDate);
        List<TPointsTradePositionDay> tradePositionDays = getTPointsTradePositionDaySSol(tradeDate);
        String defaultDate = DateUtil.getDefaultDate();
        List<TPointsSsolDay> pointsSsolDays = new ArrayList<>();
        for (TPointsProtocolPositionDay day : protocolPositionDays) {
            String key = String.format("%s_%s", day.market_indicator,
                    day.currency);
            BigDecimal protocolTokenAmount = sumProtocolTokenAmount.get(key);
            BigDecimal lpTokenAmount = sumLpTokenAmount.get(key);
            BigDecimal tradeTokenAmount = sumTradeTokenAmount.get(key);
            BigDecimal tokenAmount = DataUtils.getAdd(protocolTokenAmount,lpTokenAmount);
            tokenAmount = DataUtils.getAdd(tokenAmount,tradeTokenAmount);
            if (tokenAmount != null && day.token_amount != null ){
                TPointsSsolDay ssolDay = new TPointsSsolDay();
                BigDecimal ratio = day.token_amount.divide(tokenAmount, MathContext.DECIMAL128).setScale(9, RoundingMode.DOWN);
                ssolDay.ratio = ratio;
                ssolDay.user_id = day.user_id;
                ssolDay.currency = day.currency;
                ssolDay.sum_token_amount = tokenAmount;
                ssolDay.market_indicator = day.market_indicator;
                ssolDay.status = ToChainStatus.DidNot.getValue();
                ssolDay.trade_date = tradeDate;
                ssolDay.create_time = defaultDate;
                pointsSsolDays.add(ssolDay);

            }else {
                log.warn("TPointsProtocolPositionDay user_id:{} key:{} sumTokenAmount:{} or po.token_amount:{} is null",day.user_id,key,tokenAmount,day.token_amount);
            }
        }
        HashMap<String,PointsUserPo> pointsUserHm = new HashMap<>();
        for (TPointsLpPositionDay day : lpPositionDays) {
            String key = String.format("%s_%s", day.market_indicator,
                    day.currency);
            if (day.token_amount != null) {
                PointsUserPo po = pointsUserHm.get(key);
                if (po == null) {
                    po = new PointsUserPo();
                    po.currency = day.currency;
                    po.market_indicator = day.market_indicator;
                    po.user_id = day.user_id;
                    po.trade_date = tradeDate;
                    pointsUserHm.put(key, po);
                }
                po.token_amount = DataUtils.getAdd(day.token_amount, day.token_amount);
            }

        }
        for (TPointsTradePositionDay day : tradePositionDays) {
            String key = String.format("%s_%s", day.market_indicator,
                    day.currency);
            if (day.token_amount != null) {
                PointsUserPo po = pointsUserHm.get(key);
                if (po == null) {
                    po = new PointsUserPo();
                    po.currency = day.currency;
                    po.market_indicator = day.market_indicator;
                    po.user_id = day.user_id;
                    po.trade_date = tradeDate;
                    pointsUserHm.put(key, po);
                }
                po.token_amount = DataUtils.getAdd(day.token_amount, day.token_amount);
            }
        }
        Collection<PointsUserPo> pointsUserValues = pointsUserHm.values();
        for (PointsUserPo po : pointsUserValues) {
            String key = String.format("%s_%s", po.market_indicator,
                    po.currency);
            BigDecimal protocolTokenAmount = sumProtocolTokenAmount.get(key);
            BigDecimal lpTokenAmount = sumLpTokenAmount.get(key);
            BigDecimal tradeTokenAmount = sumTradeTokenAmount.get(key);
            BigDecimal tokenAmount = DataUtils.getAdd(protocolTokenAmount,lpTokenAmount);
            tokenAmount = DataUtils.getAdd(tokenAmount,tradeTokenAmount);
            if (tokenAmount != null && po.token_amount != null ){
                TPointsSsolDay ssolDay = new TPointsSsolDay();
                BigDecimal ratio = po.token_amount.divide(tokenAmount, MathContext.DECIMAL128).setScale(9, RoundingMode.DOWN);
                ssolDay.ratio = ratio;
                ssolDay.user_id = po.user_id;
                ssolDay.currency = po.currency;
                ssolDay.sum_token_amount = tokenAmount;
                ssolDay.market_indicator = po.market_indicator;
                ssolDay.status = ToChainStatus.DidNot.getValue();
                ssolDay.trade_date = tradeDate;
                ssolDay.create_time = defaultDate;
                pointsSsolDays.add(ssolDay);
            }else {
                log.warn("PointsUser user_id:{} key:{} sumTokenAmount:{} or po.token_amount:{} is null",po.user_id,key,tokenAmount,po.token_amount);
            }
        }
        insertTPointsSsolDay(pointsSsolDays,tradeDate);
    }


    private List<TPointsProtocolPositionDay> getTPointsProtocolPositionDayBySSol(String tradeDate) throws Exception {
        String sql ="select * from dc_points_protocol_position_day where left(trade_date,10) =? and token_amount > 0 and currency ='sSOL' and market_indicator='SONIC'";
        List<TPointsProtocolPositionDay> list = DBUtils.queryListThrowsException(sql, new Object[]{tradeDate}, TPointsProtocolPositionDay.class);
        return list;
    }

    private List<TPointsLpPositionDay> getTPointsLpPositionDayBySSol(String tradeDate) throws Exception {
        String sql ="select * from dc_points_lp_position_day where left(trade_date,10) =? and token_amount is not null and currency ='sSOL' and market_indicator='SONIC'";
        List<TPointsLpPositionDay> list = DBUtils.queryListThrowsException(sql, new Object[]{tradeDate}, TPointsLpPositionDay.class);
        return list;
    }

    private List<TPointsTradePositionDay> getTPointsTradePositionDaySSol(String tradeDate) throws Exception {
        String sql ="select * from dc_points_trade_position_day where left(trade_date,10) =? and token_amount is not null  and currency ='sSOL' and market_indicator='SONIC'";
        List<TPointsTradePositionDay> list = DBUtils.queryListThrowsException(sql, new Object[]{tradeDate}, TPointsTradePositionDay.class);
        return list;
    }


    private List<TPointsProtocolPositionDay> getTPointsProtocolPositionDayByHour(String tradeDate) throws Exception {
        String sql ="select user_id,currency,market_indicator, sum(token_amount / 24) as token_amount from dc_points_protocol_position_hour where left(trade_date,10) =? and token_amount is not null  group by user_id,currency,market_indicator";
        List<TPointsProtocolPositionDay> list = DBUtils.queryListThrowsException(sql, new Object[]{tradeDate}, TPointsProtocolPositionDay.class);
        return list;
    }

    private List<TPointsLpPositionDay> getTPointsLpPositionDayByHour(String tradeDate) throws Exception {
        String sql ="select user_id,currency,market_indicator, sum(token_amount / 24) as token_amount from dc_points_lp_position_hour where left(trade_date,10) =? and token_amount is not null  group by user_id,currency,market_indicator";
        List<TPointsLpPositionDay> list = DBUtils.queryListThrowsException(sql, new Object[]{tradeDate}, TPointsLpPositionDay.class);
        return list;
    }

    private List<TPointsTradePositionDay> getTPointsTradePositionDayByHour(String tradeDate) throws Exception {
        String sql ="select user_id,currency,market_indicator, sum(token_amount / 24) as token_amount from dc_points_trade_position_hour where left(trade_date,10) =? and token_amount is not null  group by user_id,currency,market_indicator";
        List<TPointsTradePositionDay> list = DBUtils.queryListThrowsException(sql, new Object[]{tradeDate}, TPointsTradePositionDay.class);
        return list;
    }



    private HashMap<String, BigDecimal> getSumProtocolTokenAmount(String tradeDate) throws Exception {
        String sql ="select market_indicator,currency,sum(token_amount) as token_amount from dc_points_protocol_position_day where left(trade_date,10) =? group by market_indicator,currency";
        List<TPointsProtocolPositionDay> protocolPositionDays = DBUtils.queryListThrowsException(sql, new Object[]{tradeDate}, TPointsProtocolPositionDay.class);
        HashMap<String, BigDecimal> sumProtocolTokenAmount = new HashMap<>();
        for (TPointsProtocolPositionDay day : protocolPositionDays) {
            String key = String.format("%s_%s", day.market_indicator,
                    day.currency);
            sumProtocolTokenAmount.put(key, day.token_amount);
        }
        return sumProtocolTokenAmount;
    }

    private HashMap<String, BigDecimal> getSumLPTokenAmount(String tradeDate) throws Exception {
        String sql ="select market_indicator,currency,sum(token_amount) as token_amount from dc_points_lp_position_day where left(trade_date,10) =? group by market_indicator,currency";
        List<TPointsLpPositionDay> lpPositionDays = DBUtils.queryListThrowsException(sql, new Object[]{tradeDate}, TPointsLpPositionDay.class);
        HashMap<String, BigDecimal> sumLpTokenAmount = new HashMap<>();
        for (TPointsLpPositionDay day : lpPositionDays) {
            String key = String.format("%s_%s", day.market_indicator,
                    day.currency);
            sumLpTokenAmount.put(key, day.token_amount);
        }
        return sumLpTokenAmount;
    }

    private HashMap<String, BigDecimal> getSumTradeTokenAmount(String tradeDate) throws Exception {
        String sql ="select market_indicator,currency,sum(token_amount) as token_amount from dc_points_trade_position_day where left(trade_date,10) =? group by market_indicator,currency";
        List<TPointsTradePositionDay> tradePositionDays = DBUtils.queryListThrowsException(sql, new Object[]{tradeDate}, TPointsTradePositionDay.class);
        HashMap<String, BigDecimal> sumTradeTokenAmount = new HashMap<>();
        for (TPointsTradePositionDay day : tradePositionDays) {
            String key = String.format("%s_%s", day.market_indicator,
                    day.currency);
            sumTradeTokenAmount.put(key, day.token_amount);
        }
        return sumTradeTokenAmount;
    }

    private void insertTPointsLpPositionDay(List<TPointsLpPositionDay> tPointsLpPositionDays, String tradeDate) throws Exception {
        String defaultDate = DateUtil.getDefaultDate();
        if (tPointsLpPositionDays != null && tPointsLpPositionDays.size() > 0) {
            for (TPointsLpPositionDay day : tPointsLpPositionDays){
                day.trade_date = tradeDate;
                day.create_time = defaultDate;
            }
            String sql = "delete from dc_points_lp_position_day where trade_date=?";
            ExecTablePo deExecTablePo = new ExecTablePo();
            deExecTablePo.sql = sql;
            deExecTablePo.args = new Object[] { tradeDate};
            deExecTablePo.type = ExceType.delete;

            ExecTablePo<TPointsLpPositionDay> insert = new ExecTablePo();
            insert.tableName ="dc_points_lp_position_day";
            insert.insertlist = tPointsLpPositionDays;
            insert.type = ExceType.insertList;

            List<ExecTablePo> execTablePoList = new ArrayList<>();
            execTablePoList.add(deExecTablePo);
            execTablePoList.add(insert);
            int num = tranSql(execTablePoList);
            log.info("insertTPointsLpPositionDay size:{} tranSql number:{}", tPointsLpPositionDays.size(),num);
        }
        log.info("insertTPointsLpPositionDay:{} size:{}", tradeDate, tPointsLpPositionDays.size());
    }


    private void insertTPointsProtocolPositionDay(List<TPointsProtocolPositionDay> tPointsProtocolPositionDays, String tradeDate) throws Exception {
        String defaultDate = DateUtil.getDefaultDate();
        if (tPointsProtocolPositionDays != null && tPointsProtocolPositionDays.size() > 0) {
            for (TPointsProtocolPositionDay day : tPointsProtocolPositionDays){
                day.trade_date = tradeDate;
                day.create_time = defaultDate;
            }
            String sql = "delete from dc_points_protocol_position_day where trade_date=?";
            ExecTablePo deExecTablePo = new ExecTablePo();
            deExecTablePo.sql = sql;
            deExecTablePo.args = new Object[] { tradeDate};
            deExecTablePo.type = ExceType.delete;

            ExecTablePo<TPointsProtocolPositionDay> insert = new ExecTablePo();
            insert.tableName ="dc_points_protocol_position_day";
            insert.insertlist = tPointsProtocolPositionDays;
            insert.type = ExceType.insertList;

            List<ExecTablePo> execTablePoList = new ArrayList<>();
            execTablePoList.add(deExecTablePo);
            execTablePoList.add(insert);
            int num = tranSql(execTablePoList);
            log.info("insertTPointsProtocolPositionDay size:{} tranSql number:{}", tPointsProtocolPositionDays.size(),num);
        }
        log.info("insertTPointsProtocolPositionDay:{} size:{}", tradeDate, tPointsProtocolPositionDays.size());
    }



    private void insertTPointsTradePositionDay(List<TPointsTradePositionDay> tradePositionDays, String tradeDate) throws Exception {
        String defaultDate = DateUtil.getDefaultDate();
        if (tradePositionDays != null && tradePositionDays.size() > 0) {
            for (TPointsTradePositionDay day : tradePositionDays){
                day.trade_date = tradeDate;
                day.create_time = defaultDate;
            }
            String sql = "delete from dc_points_trade_position_day where trade_date=?";
            ExecTablePo deExecTablePo = new ExecTablePo();
            deExecTablePo.sql = sql;
            deExecTablePo.args = new Object[] { tradeDate};
            deExecTablePo.type = ExceType.delete;

            ExecTablePo<TPointsTradePositionDay> insert = new ExecTablePo();
            insert.tableName ="dc_points_trade_position_day";
            insert.insertlist = tradePositionDays;
            insert.type = ExceType.insertList;

            List<ExecTablePo> execTablePoList = new ArrayList<>();
            execTablePoList.add(deExecTablePo);
            execTablePoList.add(insert);
            int num = tranSql(execTablePoList);
            log.info("insertTPointsTradePositionDay size:{} tranSql number:{}", tradePositionDays.size(),num);
        }
        log.info("insertTPointsTradePositionDay:{} size:{}", tradeDate, tradePositionDays.size());
    }


    private void insertTPointsSsolDay(List<TPointsSsolDay> tPointsSsolDays, String tradeDate) throws Exception {
        if (tPointsSsolDays != null && tPointsSsolDays.size() > 0) {
            String sql = "delete from dc_points_ssol_day where trade_date=?";
            ExecTablePo deExecTablePo = new ExecTablePo();
            deExecTablePo.sql = sql;
            deExecTablePo.args = new Object[] { tradeDate};
            deExecTablePo.type = ExceType.delete;

            ExecTablePo<TPointsSsolDay> insert = new ExecTablePo();
            insert.tableName ="dc_points_ssol_day";
            insert.insertlist = tPointsSsolDays;
            insert.type = ExceType.insertList;

            List<ExecTablePo> execTablePoList = new ArrayList<>();
            execTablePoList.add(deExecTablePo);
            execTablePoList.add(insert);
            int num = tranSql(execTablePoList);
            log.info("insertTPointsSsolDay size:{} tranSql number:{}", tPointsSsolDays.size(),num);
        }
        log.info("insertTPointsSsolDay:{} size:{}", tradeDate, tPointsSsolDays.size());
    }






    public static String getDate() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String date;
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, -1);
        date = format.format(cal.getTime());
        return date;
    }
}
