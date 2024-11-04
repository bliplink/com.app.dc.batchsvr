package com.app.dc.service.job;

import com.alibaba.fastjson.JSON;
import com.app.common.db.DBUtils;
import com.app.common.utils.JsonUtils;
import com.app.common.utils.PResult;
import com.app.common.utils.StringUtil;
import com.app.dc.data.Symbol;
import com.app.dc.engine.po.Position;
import com.app.dc.entity.*;
import com.app.dc.enums.ExceType;
import com.app.dc.enums.ToChainStatus;
import com.app.dc.fix.message.MarketDataSnapshotFullRefresh;
import com.app.dc.po.LiquidationParam;
import com.app.dc.po.SolanaCategoryDPrice;
import com.app.dc.price.MarketPrice;
import com.app.dc.service.BatchStart;
import com.app.dc.util.Consts;
import com.app.dc.util.DataUtils;
import com.app.dc.util.ObjectConvert;
import com.app.dc.utils.DateUtil;
import com.app.dc.utils.PositionUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class YesterdaysDataJob extends TranSQL implements Job {

    @Setter
    private BatchStart batchStart;

    private static boolean runFlag = false;
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        log.info("AccountTotalBalanceJob execute");
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
                log.info("YesterdaysDataJob Job start");
                String date = getDate();
                String lastDate = getLastDate();
                log.info("YesterdaysDataJob date:{} lastDate:{}",date,lastDate);
                getYesterdaysData(date);
                HashMap<String,TSymbolPriceDay> exchangeRateHm = getTSymbolPriceDayByDate(date);
                List<TUsers> users = getAllUsers();
                List<TKols> kols = getAllTKols();
                try {
                    calTotalBalance(date,lastDate,exchangeRateHm);
                }catch (Exception e){
                    log.error("calTotalBalance",e);
                }
                try {
                    calRakeBack(date, exchangeRateHm, users, kols);
                }catch (Exception e){
                    log.error("calRakeBack",e);
                }
                log.info("YesterdaysDataJob Job end");
            } catch (Exception e) {
                log.error("YesterdaysDataJob error", e);
            }finally {
                runFlag = false;
            }
        } else {
            log.warn("YesterdaysDataJob job is runing.");

        }
    }

    private void getYesterdaysData(String date) throws Exception {
        HashMap<String, MarketPrice> marketPriceHashMap = batchStart.marketPriceFacade.getMarketPriceMap();
        updateTOrdersPositionToDay(date,marketPriceHashMap);
        updateTUsersBalanceToDay(date);
        updateTSymbolPriceDayToDay(date);

    }

    public void calTotalBalance(String date,String lastDate,HashMap<String,TSymbolPriceDay> exchangeRateHm) throws Exception {
        List<TOrdersPositionDay> ordersPositionDays = getTOrdersPositionByDate(date);
        List<TUsersBalanceDay> usersBalanceDays = getTUsersBalanceByDate(date);

//        List<TOrdersExecorders> usersSumDateRealizedPnls = getUsersSumRealizedPnlOrderByDate(date);
        HashMap<String,TOrdersExecorders> todayFeeHm = getSumFee(date);
        List<TOrdersExecorders> todayFees = new ArrayList<>();
        todayFees.addAll(todayFeeHm.values());
        List<TUsersCashLog> todayCashLogs = getSumCashLog(date);
//        List<TOrdersExecorders> usersALLSumRealizedPnls = getAllUsersSumRealizedPnlOrder();
        Map<String,TUsersTotalBalance> lastUsersTotalBalanceHm = getTUsersTotalBalance(lastDate);
        Map<String,TUsersMarketTotalBalance> lastUsersMarketTotalBalanceHm = getTUsersMarketTotalBalance(lastDate);
        Map<String,TUsersMarketTotalBalance> usersMarketTotalBalanceHm = new HashMap<>();
        for (TOrdersPositionDay day : ordersPositionDays) {
            Position p = ObjectConvert.ordersPositionToPosition(day);
            Symbol symbol = batchStart.dataFacade.getSymbol2(day.symbol);
            if (symbol == null){
                log.warn("symbol is null. symbol:{} date:{}",day.symbol,date);
                continue;
            }
            BigDecimal pnl = PositionUtils.calPnlByPositionRate(day.last_price,day.rate_price,p);
            if (pnl == null){
                log.warn("UserID:{} AccountId:{} securityID:{} date:{} pnl is null", p.getUserID(), p.getAccountID(), p.getSecurityID(),date);
                continue;
            }
            String key = day.user_id+day.market_indicator;
            TUsersMarketTotalBalance tUsersMarketTotalBalance = usersMarketTotalBalanceHm.get(key);
            if (tUsersMarketTotalBalance == null){
                tUsersMarketTotalBalance = new TUsersMarketTotalBalance();
                tUsersMarketTotalBalance.user_id = day.user_id;
                tUsersMarketTotalBalance.market_indicator = day.market_indicator;
                tUsersMarketTotalBalance.trade_date = date;
                String securityID = symbol.getSymbolLevel1Category()+""+batchStart.getConvertCurrency();
                TSymbolPriceDay marketDate = exchangeRateHm.get(securityID);
                if (marketDate != null && marketDate.price != null) {
                    tUsersMarketTotalBalance.exchange_rate = marketDate.price;
                }else {
                    log.warn("TOrdersPositionDay exchange_rate is null UserID:{} AccountId:{} securityID:{}  exchange_rateSecurityID:{} date:{}", p.getUserID(), p.getAccountID(), p.getSecurityID(),securityID,date);
                    continue;
                }
                tUsersMarketTotalBalance.unrealized_pnl = pnl;
                usersMarketTotalBalanceHm.put(key,tUsersMarketTotalBalance);
            }else {
                if (tUsersMarketTotalBalance.unrealized_pnl != null) {
                    tUsersMarketTotalBalance.unrealized_pnl = tUsersMarketTotalBalance.unrealized_pnl.add(pnl);
                }else {
                    tUsersMarketTotalBalance.unrealized_pnl = pnl;
                }
            }

        }

        for (TUsersBalanceDay day : usersBalanceDays) {
            String key = day.user_id+day.market_indicator;
            TUsersMarketTotalBalance tUsersMarketTotalBalance = usersMarketTotalBalanceHm.get(key);
            if (tUsersMarketTotalBalance == null){
                tUsersMarketTotalBalance = new TUsersMarketTotalBalance();
                tUsersMarketTotalBalance.user_id = day.user_id;
                tUsersMarketTotalBalance.market_indicator = day.market_indicator;
                tUsersMarketTotalBalance.trade_date = date;
                String securityID = day.market_indicator+""+batchStart.getConvertCurrency();
//                log.info("TUsersBalanceDay ConvertCurrency exchange_rate SecurityID:{}",securityID);
                TSymbolPriceDay marketDate = exchangeRateHm.get(securityID);
                if (marketDate != null && marketDate.price != null) {
                    tUsersMarketTotalBalance.exchange_rate = marketDate.price;
                }else {
                    log.warn("TUsersBalanceDay exchange_rate is null UserID:{} AccountId:{} exchange_rateSecurityID:{} date:{}", day.user_id, day.account_id,securityID,date);
                    continue;
                }
                tUsersMarketTotalBalance.sum_balance = day.balance;
                usersMarketTotalBalanceHm.put(key,tUsersMarketTotalBalance);
            }else {
                if (tUsersMarketTotalBalance.sum_balance == null){
                    tUsersMarketTotalBalance.sum_balance = day.balance;
                }else {
                    tUsersMarketTotalBalance.sum_balance = tUsersMarketTotalBalance.sum_balance.add(day.balance);
                }
            }
        }
        //sum toaday_total_deposit_amount- open fee, toaday_total_withdrawal_amount
        for (TUsersCashLog cashLog : todayCashLogs) {
            String key = cashLog.user_id + cashLog.market_indicator;
            TUsersMarketTotalBalance tUsersMarketTotalBalance = usersMarketTotalBalanceHm.get(key);
            if (tUsersMarketTotalBalance == null) {
                tUsersMarketTotalBalance = new TUsersMarketTotalBalance();
                tUsersMarketTotalBalance.user_id = cashLog.user_id;
                tUsersMarketTotalBalance.market_indicator = cashLog.market_indicator;
                tUsersMarketTotalBalance.trade_date = date;
                String securityID = cashLog.market_indicator + "" + batchStart.getConvertCurrency();
//                log.info("TUsersBalanceDay ConvertCurrency exchange_rate SecurityID:{}",securityID);
                TSymbolPriceDay marketDate = exchangeRateHm.get(securityID);
                if (marketDate != null && marketDate.price != null) {
                    tUsersMarketTotalBalance.exchange_rate = marketDate.price;
                } else {
                    log.warn("TUsersCashLog exchange_rate is null UserID:{} exchange_rateSecurityID:{} date:{}", cashLog.user_id, securityID, date);
                    continue;
                }
                usersMarketTotalBalanceHm.put(key, tUsersMarketTotalBalance);
            }
            if (cashLog.amount != null) {
                if (cashLog.side == 0) {
                    String feeKey = cashLog.user_id+cashLog.market_indicator+"0";
                    BigDecimal amount = cashLog.amount;
                    TOrdersExecorders ex = todayFeeHm.get(feeKey);
                    if (ex != null && ex.fee != null){
                        BigDecimal subAmount = amount.subtract(ex.fee);
                        log.info("TUsersCashLog amount:{} fee:{} subAmount:{}",amount,ex.fee,subAmount);
                        amount = subAmount;
                    }
                    tUsersMarketTotalBalance.today_total_deposit_amount = DataUtils.getAdd(tUsersMarketTotalBalance.today_total_deposit_amount,amount);

                }else {
                    tUsersMarketTotalBalance.today_total_withdrawal_amount = DataUtils.getAdd(tUsersMarketTotalBalance.today_total_withdrawal_amount,cashLog.amount);
                }
            }
        }
        // cal close fee
        for (TOrdersExecorders ex : todayFees) {
            if (ex.oc_type.equals("1")) {
                String key = ex.user_id + ex.market_indicator;
                TUsersMarketTotalBalance tUsersMarketTotalBalance = usersMarketTotalBalanceHm.get(key);
                if (tUsersMarketTotalBalance == null) {
                    tUsersMarketTotalBalance = new TUsersMarketTotalBalance();
                    tUsersMarketTotalBalance.user_id = ex.user_id;
                    tUsersMarketTotalBalance.market_indicator = ex.market_indicator;
                    tUsersMarketTotalBalance.trade_date = date;
                    String securityID = ex.market_indicator + "" + batchStart.getConvertCurrency();
//                log.info("TUsersBalanceDay ConvertCurrency exchange_rate SecurityID:{}",securityID);
                    TSymbolPriceDay marketDate = exchangeRateHm.get(securityID);
                    if (marketDate != null && marketDate.price != null) {
                        tUsersMarketTotalBalance.exchange_rate = marketDate.price;
                    } else {
                        log.warn("TOrdersExecorders close fee, exchange_rate is null UserID:{} exchange_rateSecurityID:{} date:{}", ex.user_id,securityID, date);
                        continue;
                    }
                    usersMarketTotalBalanceHm.put(key, tUsersMarketTotalBalance);
                }
                tUsersMarketTotalBalance.today_total_close_fee = DataUtils.getAdd(tUsersMarketTotalBalance.today_total_close_fee,ex.fee);

            }
        }

        List<TUsersMarketTotalBalance> tUsersMarketTotalBalanceList = new ArrayList<>();
        tUsersMarketTotalBalanceList.addAll(usersMarketTotalBalanceHm.values());
        String create_time = DateUtil.getDefaultDate();
        for (TUsersMarketTotalBalance tb : tUsersMarketTotalBalanceList) {
            String key = tb.user_id + tb.market_indicator;
            TUsersMarketTotalBalance lastTb = lastUsersMarketTotalBalanceHm.get(key);
            if (lastTb == null) {
                TUsersMarketTotalBalance newTb = new TUsersMarketTotalBalance();
                newTb.exchange_rate = tb.exchange_rate;
                newTb.market_indicator = tb.market_indicator;
                newTb.realized_pnl = DataUtils.getScaleDOWN(tb.realized_pnl);
                newTb.sum_balance = DataUtils.getScaleDOWN(tb.sum_balance);
                newTb.sum_realized_pnl = DataUtils.getScaleDOWN(tb.sum_realized_pnl);
                newTb.today_total_close_fee = DataUtils.getScaleDOWN(tb.today_total_close_fee);
                newTb.today_total_deposit_amount = DataUtils.getScaleDOWN(tb.today_total_deposit_amount);
                newTb.today_total_withdrawal_amount = DataUtils.getScaleDOWN(tb.today_total_withdrawal_amount);
                newTb.trade_date = date;
                newTb.unrealized_pnl = DataUtils.getScaleDOWN(tb.unrealized_pnl);
                newTb.user_id = tb.user_id;
                newTb.create_time = create_time;
                lastUsersMarketTotalBalanceHm.put(key, newTb);
            } else {
                lastTb.exchange_rate = tb.exchange_rate;
                lastTb.trade_date = date;
                lastTb.create_time = create_time;
                lastTb.unrealized_pnl = DataUtils.getScaleDOWN(tb.unrealized_pnl);
//                    lastTb.realized_pnl = DataUtils.getScaleDOWN(tb.realized_pnl);
                lastTb.sum_balance = DataUtils.getScaleDOWN(tb.sum_balance);
//                    lastTb.sum_realized_pnl = DataUtils.getScaleDOWN(tb.sum_realized_pnl);
                lastTb.today_total_deposit_amount = DataUtils.getScaleDOWN(tb.today_total_deposit_amount);
                lastTb.today_total_withdrawal_amount = DataUtils.getScaleDOWN(tb.today_total_withdrawal_amount);
                lastTb.today_total_close_fee = DataUtils.getScaleDOWN(tb.today_total_close_fee);
            }

        }
        HashMap<String,TUsersTotalBalance> tUsersTotalBalanceMap = new HashMap<>();
        for (TUsersMarketTotalBalance usersBalanceDay : tUsersMarketTotalBalanceList) {
            BigDecimal sum_balance = null;
            BigDecimal sum_pnl = null;
            BigDecimal toaday_total_deposit_amount = null;
            BigDecimal toaday_total_withdrawal_amount = null;
            BigDecimal toaday_total_close_fee = null;
            if (usersBalanceDay.exchange_rate != null) {
                if (usersBalanceDay.sum_balance != null) {
                    sum_balance = usersBalanceDay.sum_balance.multiply(usersBalanceDay.exchange_rate);
                }

                if (usersBalanceDay.unrealized_pnl != null) {
                    sum_pnl = usersBalanceDay.unrealized_pnl.multiply(usersBalanceDay.exchange_rate);
                }

                if (usersBalanceDay.today_total_deposit_amount != null) {
                    toaday_total_deposit_amount = usersBalanceDay.today_total_deposit_amount.multiply(usersBalanceDay.exchange_rate);
                }

                if (usersBalanceDay.today_total_withdrawal_amount != null) {
                    toaday_total_withdrawal_amount = usersBalanceDay.today_total_withdrawal_amount.multiply(usersBalanceDay.exchange_rate);
                }

                if (usersBalanceDay.today_total_close_fee != null) {
                    toaday_total_close_fee = usersBalanceDay.today_total_close_fee.multiply(usersBalanceDay.exchange_rate);
                }
            }

            TUsersTotalBalance tUsersTotalBalance = tUsersTotalBalanceMap.get(usersBalanceDay.user_id);
            if (tUsersTotalBalance == null) {
                tUsersTotalBalance = new TUsersTotalBalance();
                tUsersTotalBalance.user_id = usersBalanceDay.user_id;
                tUsersTotalBalance.trade_date = date;
                tUsersTotalBalanceMap.put(tUsersTotalBalance.user_id, tUsersTotalBalance);
            }
            tUsersTotalBalance.create_time = create_time;
            tUsersTotalBalance.total_balance = DataUtils.getAdd(tUsersTotalBalance.total_balance,sum_pnl);
            tUsersTotalBalance.total_balance = DataUtils.getAdd(tUsersTotalBalance.total_balance, sum_balance);
            tUsersTotalBalance.today_total_deposit_amount = DataUtils.getAdd(tUsersTotalBalance.today_total_deposit_amount,toaday_total_deposit_amount);
            tUsersTotalBalance.today_total_withdrawal_amount = DataUtils.getAdd(tUsersTotalBalance.today_total_withdrawal_amount,toaday_total_withdrawal_amount);
            tUsersTotalBalance.today_total_close_fee = DataUtils.getAdd(tUsersTotalBalance.today_total_close_fee,toaday_total_close_fee);
        }
        List<TUsersTotalBalance> tUsersTotalBalances = new ArrayList<>();
        tUsersTotalBalances.addAll(tUsersTotalBalanceMap.values());
        List<TUsersTotalBalance> InsertUsersTotalBalances = new ArrayList<>();
        for (TUsersTotalBalance usersBalanceDay : tUsersTotalBalances) {
            BigDecimal today_pnl = usersBalanceDay.total_balance;
            if (today_pnl == null){
                today_pnl = BigDecimal.ZERO;
            }
            if (usersBalanceDay.today_total_deposit_amount != null) {
                today_pnl = today_pnl.subtract(usersBalanceDay.today_total_deposit_amount);
            }
            today_pnl = DataUtils.getAdd(today_pnl,usersBalanceDay.today_total_withdrawal_amount);
            today_pnl = DataUtils.getAdd(today_pnl,usersBalanceDay.today_total_close_fee);
            if (lastUsersTotalBalanceHm.containsKey(usersBalanceDay.user_id)){
                TUsersTotalBalance lastUsersTotalBalance = lastUsersTotalBalanceHm.get(usersBalanceDay.user_id);
                lastUsersTotalBalance.trade_date = usersBalanceDay.trade_date;
                lastUsersTotalBalance.create_time = create_time;
                if (today_pnl != null && lastUsersTotalBalance.total_balance != null){
                    today_pnl = today_pnl.subtract(lastUsersTotalBalance.total_balance);
                }

                lastUsersTotalBalance.total_balance = usersBalanceDay.total_balance;
                lastUsersTotalBalance.today_total_deposit_amount = DataUtils.getScaleDOWN(usersBalanceDay.today_total_deposit_amount);

                lastUsersTotalBalance.today_total_withdrawal_amount = DataUtils.getScaleDOWN(usersBalanceDay.today_total_withdrawal_amount);

                lastUsersTotalBalance.today_total_close_fee = DataUtils.getScaleDOWN(usersBalanceDay.today_total_close_fee);

                lastUsersTotalBalance.today_total_pnl =DataUtils.getScaleDOWN(today_pnl);

                lastUsersTotalBalance.total_pnl = DataUtils.getAdd(lastUsersTotalBalance.today_total_pnl,lastUsersTotalBalance.total_pnl);
                lastUsersTotalBalance.total_pnl = DataUtils.getScaleDOWN(lastUsersTotalBalance.total_pnl);

            }else {
                TUsersTotalBalance newPO = new TUsersTotalBalance();
                newPO.user_id = usersBalanceDay.user_id;
                newPO.trade_date = usersBalanceDay.trade_date;
                if (usersBalanceDay.total_balance != null){
                    newPO.total_balance = DataUtils.getScaleDOWN(usersBalanceDay.total_balance);
                }
                newPO.today_total_deposit_amount = DataUtils.getScaleDOWN(usersBalanceDay.today_total_deposit_amount);

                newPO.today_total_withdrawal_amount = DataUtils.getScaleDOWN(usersBalanceDay.today_total_withdrawal_amount);
                newPO.today_total_close_fee = DataUtils.getScaleDOWN(usersBalanceDay.today_total_close_fee);
                if (today_pnl != null){
                    newPO.total_pnl = DataUtils.getScaleDOWN(today_pnl);
                    newPO.today_total_pnl = DataUtils.getScaleDOWN(today_pnl);
                }
                newPO.create_time = create_time;
                InsertUsersTotalBalances.add(newPO);
            }

        }

        List<TUsersMarketTotalBalance> tUsersMarketTotalBalanceList1 = new ArrayList<>();
        tUsersMarketTotalBalanceList1.addAll(lastUsersMarketTotalBalanceHm.values());
        for (TUsersMarketTotalBalance po : tUsersMarketTotalBalanceList1){
            po.trade_date = date;
            po.create_time = create_time;
        }
        updateTUsersMarketTotalBalance(tUsersMarketTotalBalanceList1,date);
        InsertUsersTotalBalances.addAll(lastUsersTotalBalanceHm.values());
        updateTUsersTotalBalance(InsertUsersTotalBalances,date);

    }


    public void calRakeBack(String date,HashMap<String,TSymbolPriceDay> exchangeRateHm,List<TUsers> usersList,List<TKols> kolsList) throws Exception{
        log.info("rakeBackToChainEnabled:{} RakeBackConvert:{}",batchStart.isRakeBackToChainEnabled(),batchStart.getRakeBackConvert());
        HashMap<String, TKols> kolsReferrerHm = new HashMap<>();
        for (TKols tKolPartner : kolsList) {
            kolsReferrerHm.put(tKolPartner.referrer, tKolPartner);
        }
        //Invitation Count, kol_user_id is key
        HashMap<String,Integer> todayInvitedCountHm = new HashMap<>();
        HashMap<String, TUsers> hmUsers = new HashMap<>();
        for (TUsers tUsers : usersList) {
            hmUsers.put(tUsers.user_id, tUsers);
            String cDate = tUsers.create_time.substring(0,10);
            if (StringUtil.isNotEmpty(tUsers.referrer) && cDate.equals(date)){
                TKols tKolPartner = kolsReferrerHm.get(tUsers.referrer);
                if (tKolPartner == null){
                    log.warn("Invitation code error, referrer:{}",tUsers.referrer);
                }else {
                    if (todayInvitedCountHm.containsKey(tKolPartner.kol_user_id)) {
                        int count = todayInvitedCountHm.get(tKolPartner.kol_user_id);
                        count = count + 1;
                        todayInvitedCountHm.put(tKolPartner.kol_user_id, count);
                    } else {
                        todayInvitedCountHm.put(tKolPartner.kol_user_id, 1);
                    }
                }

            }
        }

        List<TOrdersExecorders> sumFeeDate = getSumMarketIndicatorFeeByDate(date);
        Map<String,TOrdersExecorders> sumUDCExOrder = new HashMap<>();
        for (TOrdersExecorders execOrders : sumFeeDate) {
            String securityID = execOrders.market_indicator + "" + batchStart.getRakeBackConvert();
            TSymbolPriceDay marketDate = exchangeRateHm.get(securityID);
            if (marketDate != null && marketDate.price != null) {
                TOrdersExecorders usdcEx = sumUDCExOrder.get(execOrders.user_id);
                if (usdcEx == null){
                    usdcEx = new TOrdersExecorders();
                    usdcEx.user_id = execOrders.user_id;
                    sumUDCExOrder.put(usdcEx.user_id, usdcEx);
                }
                if (execOrders.fee != null) {
                    BigDecimal fee = execOrders.fee.multiply(marketDate.price);
                    usdcEx.fee = DataUtils.getAdd(fee, usdcEx.fee);

                }
                if (execOrders.last_qty != null){
                    BigDecimal qty = execOrders.last_qty.multiply(marketDate.price);
                    usdcEx.last_qty = DataUtils.getAdd(qty, usdcEx.last_qty);
                }
            } else {
                log.warn("TOrdersExecorders exchange_rate is null UserID:{} exchange_rateSecurityID:{} date:{}", execOrders.user_id, securityID, date);
                continue;
            }
        }
        Set<String> userIds = sumUDCExOrder.keySet();
        String create_time = DateUtil.getDefaultDate();
        List<TKolsRptDetail> details = new ArrayList<>();
        for (String userId : userIds) {
            TOrdersExecorders usdcEx = sumUDCExOrder.get(userId);
            if (usdcEx != null && usdcEx.fee != null) {
                TUsers users = hmUsers.get(userId);
                if (users != null) {
                    String referrer1 = users.referrer;
                    // Direct signing users
                    if (StringUtils.isBlank(referrer1)) {
                        continue;
                    }
                    TKols tKol1 = kolsReferrerHm.get(referrer1);
                    if (tKol1 != null) {
                        log.info(tKol1.toString());
                    }
                    if (tKol1 != null && tKol1.level1_rebate != null) {
                        TKolsRptDetail detail1 = new TKolsRptDetail();
                        BigDecimal feeAmount = usdcEx.fee.multiply(tKol1.level1_rebate);
                        feeAmount = feeAmount.setScale(9, RoundingMode.DOWN);
                        log.info("type 1 kol_user_id:{} userId:{} usdcEx.fee:{} feeAmount:{}",tKol1.kol_user_id,userId,usdcEx.fee,feeAmount);
                        detail1.create_time = create_time;
                        detail1.update_time = create_time;
                        detail1.kol_user_id = tKol1.kol_user_id;
                        detail1.type = "1";
                        detail1.trade_date = date;
                        detail1.fee = usdcEx.fee;
                        detail1.volume = usdcEx.last_qty;
                        detail1.fee_amount = feeAmount;
                        detail1.user_id = userId;
                        detail1.rebate = tKol1.level1_rebate;
                        details.add(detail1);

                    }
                    TUsers users2 = hmUsers.get(tKol1.kol_user_id);
                    if (users2 == null) {
                        continue;
                    }
                    String referrer2 = users2.referrer;
                    //Second level signing user
                    if (StringUtils.isNoneBlank(referrer2)) {
                        TKols tKol2 = kolsReferrerHm.get(referrer2);
                        if (tKol2 != null && tKol2.level2_rebate != null) {
                            TKolsRptDetail detail2 = new TKolsRptDetail();
                            BigDecimal feeAmount = usdcEx.fee.multiply(tKol2.level2_rebate);
                            if (feeAmount.compareTo(BigDecimal.ZERO) > 0) {
                                feeAmount = feeAmount.setScale(9, RoundingMode.DOWN);
                                detail2.create_time = create_time;
                                detail2.update_time = create_time;
                                log.info("type 2 kol_user_id:{} userId:{} usdcEx.fee:{} feeAmount:{}",tKol2.kol_user_id,userId,usdcEx.fee,feeAmount);
                                detail2.kol_user_id = tKol2.kol_user_id;
                                detail2.trade_date = date;
                                detail2.type = "2";
                                detail2.fee = usdcEx.fee;
                                detail2.volume = usdcEx.last_qty;
                                detail2.fee_amount = feeAmount;
                                detail2.user_id = userId;
                                detail2.rebate = tKol2.level2_rebate;
                                details.add(detail2);
                            }
                        }
                    }

                }
            }
        }
        updateTKolsRptDetail(details, date);
        List<TKolsRptDay> stayOnChain = monTKolsRptDayJob(date,todayInvitedCountHm);
        monTKolsJob(date,kolsList);
        putKolsRptDayToChain(date,stayOnChain);

    }


    public static String getDate() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String date;
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, -1);
        date = format.format(cal.getTime());
        return date;
    }

    public static String getLastDate() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String date;
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, -2);
//        cal.add(Calendar.DAY_OF_YEAR, -1);
        date = format.format(cal.getTime());
        return date;
    }


    private List<TUsers> getAllUsers() throws Exception {
        String sql = "select * from dc_users where user_type='1'";
        List<TUsers> users = DBUtils.queryListThrowsException(sql, new Object[] {}, TUsers.class);
        log.info("getAllUsers size:{}", users.size());
        return users;
    }

    private List<TKols> getAllTKols() throws Exception {
        String sql = "select * from dc_kols where status='0'";
        List<TKols> kols = DBUtils.queryListThrowsException(sql, new Object[] {}, TKols.class);
        log.info("getAllTKols list size:{}", kols.size());
        return kols;
    }

    private  List<TOrdersExecorders> getUsersSumRealizedPnlOrderByDate(String date) throws Exception {
        String sql = "SELECT user_id,security_id,case market_indicator when 'JLP' then sum(realized_Pnl/rate) when 'SONIC' then sum(realized_Pnl/rate) when 'SUSD' then sum(realized_Pnl/rate) when 'BGSOL' then sum(realized_Pnl/rate) else sum(realized_Pnl) end as realized_Pnl  FROM dc_orders_execorders where left(transact_time,10)=? group by user_id,security_id,market_indicator";
        List<TOrdersExecorders> cusLt = DBUtils.queryListThrowsException(sql, new Object[]{date}, TOrdersExecorders.class);
        log.info("getUsersSumRealizedPnlOrderByDate date:{},size:{}", date, cusLt.size());
        return cusLt;
    }


    private List<TOrdersExecorders> getAllUsersSumRealizedPnlOrder() throws Exception {
        String sql = "SELECT user_id,security_id,case market_indicator when 'JLP' then sum(realized_Pnl/rate) when 'SONIC' then sum(realized_Pnl/rate) when 'SUSD' then sum(realized_Pnl/rate) when 'BGSOL' then sum(realized_Pnl/rate) else sum(realized_Pnl) end as realized_Pnl  FROM dc_orders_execorders group by user_id,security_id,market_indicator";
        List<TOrdersExecorders> cusLt = DBUtils.queryListThrowsException(sql, new Object[]{}, TOrdersExecorders.class);
        log.info("getAllUsersSumRealizedPnlOrder ,size:{}", cusLt.size());
        return cusLt;
    }

    private List<TOrdersExecorders> getSumMarketIndicatorFeeByDate(String date) throws Exception {
        String sql = "SELECT user_id,market_indicator,sum(fee) as fee,sum(last_qty) as last_qty  FROM( select  e.user_id as user_id ,e.market_indicator as market_indicator, e.symbol as symbol,sum(e.fee * s.protocol_fee_rate) as fee,sum(e.last_qty) as last_qty  FROM dc_orders_execorders e inner join dc_symbol s on e.symbol = s.symbol and  left(e.transact_time,10)=? group by e.user_id,e.market_indicator,e.symbol) f  group by user_id,market_indicator";
        List<TOrdersExecorders> cusLt = DBUtils.queryListThrowsException(sql, new Object[]{date}, TOrdersExecorders.class);
        log.info("getSumFeeByDate date:{},size:{}",date, cusLt.size());
        return cusLt;
    }

    private HashMap<String,TOrdersExecorders> getSumFee(String date) throws Exception {
        String feeSql ="SELECT sum(fee) as fee,market_indicator,oc_type,user_id FROM dc_orders_execorders where left(transact_time,10)=? group by market_indicator,oc_type,user_id";
        List<TOrdersExecorders> execOrders = DBUtils.queryListThrowsException(feeSql,new Object[]{date}, TOrdersExecorders.class);
        log.info("getSumFee list size:{}", execOrders.size());
        HashMap<String,TOrdersExecorders> hm = new HashMap<>();
        for (TOrdersExecorders  ex : execOrders){
            String key = ex.user_id+ex.market_indicator+ex.oc_type;
            hm.put(key,ex);
        }
        log.info("getSumFee hm size:{}", hm.size());
        return hm;
    }

    private List<TUsersCashLog> getSumCashLog(String date) throws Exception {
        String cashLogSql ="SELECT sum(abs(c.amount)) as amount,c.side as side,c.market_indicator as market_indicator,c.user_id as user_id FROM dc_users_cash_log c join dc_users_mapping l on c.account_id=l.account_id and left(c.create_time,10)=? and c.status=1 group by c.side,c.market_indicator,c.user_id";
        List<TUsersCashLog> cashLogs = DBUtils.queryListThrowsException(cashLogSql,new Object[]{date}, TUsersCashLog.class);
        log.info("getSumCashLog list size:{}", cashLogs.size());
        return cashLogs;
    }


    private Map<String,TUsersTotalBalance> getTUsersTotalBalance(String lastDate) throws Exception {
        String sql = "SELECT * FROM dc_users_total_balance where trade_date=?";
        List<TUsersTotalBalance> cusLt = DBUtils.queryListThrowsException(sql, new Object[]{lastDate}, TUsersTotalBalance.class);
        Map<String,TUsersTotalBalance> hm = new HashMap<>();
        log.info("getTUsersTotalBalance list size:{}", cusLt.size());
        for (TUsersTotalBalance tb : cusLt){
            hm.put(tb.user_id,tb);
        }
        log.info("getTUsersTotalBalance hm size:{}", hm.size());
        return hm;
    }

    private Map<String,TUsersMarketTotalBalance> getTUsersMarketTotalBalance(String lastDate) throws Exception {
        String sql = "SELECT * FROM dc_users_market_total_balance where trade_date=?";
        List<TUsersMarketTotalBalance> cusLt = DBUtils.queryListThrowsException(sql, new Object[]{lastDate}, TUsersMarketTotalBalance.class);
        Map<String,TUsersMarketTotalBalance> hm = new HashMap<>();
        log.info("getTUsersMarketTotalBalance list size:{}", cusLt.size());
        for (TUsersMarketTotalBalance tb : cusLt){
            String key = tb.user_id+tb.market_indicator;
            hm.put(key,tb);
        }
        log.info("getTUsersMarketTotalBalance hm size:{}", hm.size());
        return hm;
    }

    private void updateTSymbolPriceDayToDay(String date) throws Exception {
        String count_sql = "select count(trade_date) as count from dc_symbol_price_day where trade_date=?";
        Object o = DBUtils.querySimpleObject(count_sql,new Object[]{date}, int.class);
        int count = 0;
        if (o != null){
            count = Integer.parseInt(o.toString());
        }
        if (count <= 0){
            String defaultDate = DateUtil.getDefaultDate();
            ConcurrentHashMap<String, MarketDataSnapshotFullRefresh> exchangeRateHm = batchStart.apsFacade.getExchangeRateHm();
            List<TSymbolPriceDay> list = new ArrayList<>();
            Collection<MarketDataSnapshotFullRefresh> marketDataSnapshotFullRefreshes =  exchangeRateHm.values();
            marketDataSnapshotFullRefreshes.forEach( marketData -> {
                if (StringUtil.isNotEmpty(marketData.getMarkPrice())) {
                    TSymbolPriceDay day = new TSymbolPriceDay();
                    day.price = DataUtils.getDecStr(marketData.getMarkPrice());
                    day.symbol = marketData.getSecurityID();
                    day.trade_date = date;
                    day.create_time = defaultDate;
                    day.update_time = defaultDate;
                    list.add(day);
                }
            });
            DBUtils.insertList(list,"dc_symbol_price_day");
            log.info("updateTSymbolPriceDayToDay size:{} trade_date:{}",list.size(),date);
        }
    }

    private HashMap<String,TSymbolPriceDay> getTSymbolPriceDayByDate(String date) throws Exception {
        String sql ="select * from dc_symbol_price_day where trade_date=?";
        List<TSymbolPriceDay> list = DBUtils.queryListThrowsException(sql, new Object[]{date}, TSymbolPriceDay.class);
        HashMap<String,TSymbolPriceDay> exchangeRateHm = new HashMap<>();
        log.info("getTSymbolPriceDayByDate trade_date:{} size:{}",date,list.size());
        for (TSymbolPriceDay day : list){
            exchangeRateHm.put(day.symbol,day);
        }
        log.info("getTSymbolPriceDayByDate trade_date:{} size:{}",date,exchangeRateHm.size());
        return exchangeRateHm;
    }

    private void updateTOrdersPositionToDay(String date,HashMap<String, MarketPrice> marketPriceHashMap) throws Exception {
        String count_sql = "select count(trade_date) as count from dc_orders_position_day where trade_date=?";
        Object o = DBUtils.querySimpleObject(count_sql,new Object[]{date}, int.class);
        int count = 0;
        if (o != null){
            count = Integer.parseInt(o.toString());
        }
        if (count <= 0){
            String sql ="SELECT * FROM dc_orders_position where islp!='1'";
            List<TOrdersPositionDay> list = DBUtils.queryListThrowsException(sql, null, TOrdersPositionDay.class);
            for (TOrdersPositionDay day : list){
                day.trade_date = date;
                if (marketPriceHashMap != null) {
                    MarketPrice map = marketPriceHashMap.get(day.security_id);
                    if (map != null) {
                        day.last_price = map.getTradePx();
                    }
                }
                SolanaCategoryDPrice dPrice =  batchStart.epochFacade.getDPriceBySecurityId(day.security_id);
                if (dPrice == null){
                    log.info("TOrdersPositionDay UserID:{} AccountId:{} securityID:{} date:{} SolanaCategoryDPrice is null", day.user_id, day.account_id, day.security_id,date);
                }else {
                    day.rate_price = dPrice.rate_price;
                }
            }
            DBUtils.insertList(list,"dc_orders_position_day");
            log.info("updateTOrdersPositionToDay list:{} trade_date:{}",list.size(),date);
        }
    }


    private List<TOrdersPositionDay> getTOrdersPositionByDate(String date) throws Exception {
        String sql ="select * from dc_orders_position_day where trade_date=?";
        List<TOrdersPositionDay> list = DBUtils.queryListThrowsException(sql, new Object[]{date}, TOrdersPositionDay.class);
        log.info("getTOrdersPositionByDate trade_date:{} size:{}",date,list.size());
        return list;
    }




    private void updateTUsersBalanceToDay(String date) throws Exception {
        String count_sql = "select count(trade_date) as count from dc_users_balance_day where trade_date=?";
        Object o = DBUtils.querySimpleObject(count_sql,new Object[]{date}, int.class);
        int count = 0;
        if (o != null){
            count = Integer.parseInt(o.toString());
        }
        if (count <= 0){
            String sql ="SELECT * FROM dc_users_balance";
            List<TUsersBalanceDay> list = DBUtils.queryListThrowsException(sql, null, TUsersBalanceDay.class);
            for (TUsersBalanceDay day : list){
                day.trade_date = date;
            }
            DBUtils.insertList(list,"dc_users_balance_day");
            log.info("updateTUsersBalanceToDay list:{} trade_date:{}",list.size(),date);
        }
    }


    private List<TUsersBalanceDay> getTUsersBalanceByDate(String date) throws Exception {
        String sql ="select * from dc_users_balance_day  where trade_date=?";
        List<TUsersBalanceDay> list = DBUtils.queryListThrowsException(sql, new Object[]{date}, TUsersBalanceDay.class);
        log.info("getTUsersBalanceByDate trade_date:{} size:{}",date,list.size());
        return list;
    }



    private void updateTUsersMarketTotalBalance(List<TUsersMarketTotalBalance> tUsersMarketTotalBalanceList, String date) throws Exception {
//        DBUtils.update("delete from dc_users_market_total_balance where trade_date=?",new Object[]{date});
//        DBUtils.insertList(tUsersMarketTotalBalanceList,"dc_users_market_total_balance");

        String deleteSql = "delete from dc_users_market_total_balance where trade_date=?";
        ExecTablePo deExecTablePo = new ExecTablePo();
        deExecTablePo.sql = deleteSql;
        deExecTablePo.args =new Object[]{date};
        deExecTablePo.type = ExceType.delete;

        ExecTablePo<TUsersMarketTotalBalance> insert = new ExecTablePo();
        insert.tableName ="dc_users_market_total_balance";
        insert.insertlist = tUsersMarketTotalBalanceList;
        insert.type = ExceType.insertList;
        List<ExecTablePo> execTablePoList = new ArrayList<>();
        execTablePoList.add(deExecTablePo);
        execTablePoList.add(insert);
        int num = tranSql(execTablePoList);
        log.info("updateTUsersMarketTotalBalance tradeDate:{} size:{} num:{}",date,tUsersMarketTotalBalanceList.size(),num);
    }

    private void updateTUsersTotalBalance(List<TUsersTotalBalance> tUsersTotalBalances, String date) throws Exception {
//        DBUtils.update("delete from dc_users_total_balance where trade_date=?",new Object[]{date});
//        DBUtils.insertList(tUsersTotalBalances,"dc_users_total_balance");

        String deleteSql = "delete from dc_users_total_balance where trade_date=?";
        ExecTablePo deExecTablePo = new ExecTablePo();
        deExecTablePo.sql = deleteSql;
        deExecTablePo.args =new Object[]{date};
        deExecTablePo.type = ExceType.delete;

        ExecTablePo<TUsersTotalBalance> insert = new ExecTablePo();
        insert.tableName ="dc_users_total_balance";
        insert.insertlist = tUsersTotalBalances;
        insert.type = ExceType.insertList;
        List<ExecTablePo> execTablePoList = new ArrayList<>();
        execTablePoList.add(deExecTablePo);
        execTablePoList.add(insert);
        int num = tranSql(execTablePoList);
        log.info("updateTUsersTotalBalance tradeDate:{} size:{} num:{}",date,tUsersTotalBalances.size(),num);
    }


    private void updateTKolsRptDetail(List<TKolsRptDetail> tKolsRptDetails, String date) throws Exception {
        String deleteSql = "delete from dc_kols_rpt_detail where trade_date=?";
        ExecTablePo deExecTablePo = new ExecTablePo();
        deExecTablePo.sql = deleteSql;
        deExecTablePo.args =new Object[]{date};
        deExecTablePo.type = ExceType.delete;

        ExecTablePo<TKolsRptDetail> insert = new ExecTablePo();
        insert.tableName ="dc_kols_rpt_detail";
        insert.insertlist = tKolsRptDetails;
        insert.type = ExceType.insertList;
        List<ExecTablePo> execTablePoList = new ArrayList<>();
        execTablePoList.add(deExecTablePo);
        execTablePoList.add(insert);
        int num = tranSql(execTablePoList);
        log.info("updateTKolsRptDetail tradeDate:{} size:{} num:{}",date,tKolsRptDetails.size(),num);
    }

    public List<TKolsRptDay> monTKolsRptDayJob(String date,HashMap<String,Integer> todayInvitedCountHm) throws Exception {
        List<TKolsRptDay> lt = getTKolsRptDayByRptDetail(date);
        List<TKolsRptDay> stayOnChain = new ArrayList<>();
        List<String> upperChainKol = getUpperChainTKolsRptDay(date);
        List<String> list = new ArrayList<>();
        for (TKolsRptDay day : lt){
            if (!upperChainKol.contains(day.kol_user_id)) {
                day.trade_date = date;
                if (todayInvitedCountHm.containsKey(day.kol_user_id)) {
                    int count = todayInvitedCountHm.get(day.kol_user_id);
                    day.invited_count = count;
                }
                list.add(day.kol_user_id);
                stayOnChain.add(day);
            }
        }
        Set<String> InvitedSets = todayInvitedCountHm.keySet();
        String defaultDate = DateUtil.getDefaultDate();
        // Only invitees
        for (String kol_user_id : InvitedSets){
            if (!list.contains(kol_user_id) && !upperChainKol.contains(kol_user_id)){
                TKolsRptDay day = new TKolsRptDay();
                day.kol_user_id = kol_user_id;
                day.trade_date = date;
                day.status = ToChainStatus.DidNot.getValue();
                day.update_time = day.create_time = defaultDate;
                day.invited_count = todayInvitedCountHm.get(kol_user_id);
                stayOnChain.add(day);
            }
        }

        insertStayOnChainTKolsRptDay(stayOnChain, date);
        return stayOnChain;
    }

    private List<String> getUpperChainTKolsRptDay(String date) throws Exception {
        List<String> list = new ArrayList<>();
        String selectCus = "select kol_user_id from dc_kols_rpt_day where trade_date=? and status = '1'";
        List<TKolsRptDay> cusLt = DBUtils.queryListThrowsException(selectCus, new Object[]{date}, TKolsRptDay.class);
        for (TKolsRptDay tKolsRptDay : cusLt) {
            list.add(tKolsRptDay.kol_user_id);
        }
        log.info("getUpperChainTKolsRptDay date:{} size:{}",date, list.size());

        return list;
    }

    private List<TKolsRptDay> getTKolsRptDayByRptDetail(String date) throws Exception {
        String defaultDate = DateUtil.getDefaultDate();
        String selectCus = "select kol_user_id,sum(fee_amount) as fee_amount, sum(volume) as volume from dc_kols_rpt_detail where trade_date=? group by kol_user_id";
        List<TKolsRptDay> cusLt = DBUtils.queryListThrowsException(selectCus, new Object[]{date}, TKolsRptDay.class);
        for (TKolsRptDay tKolsRptDay : cusLt) {
            tKolsRptDay.update_time = defaultDate;
            tKolsRptDay.create_time = defaultDate;
            tKolsRptDay.status = ToChainStatus.DidNot.getValue();  //Stay on chain
        }
        log.info("getTKolsRptDayByRptDetail date:{} size:{}", date,cusLt.size());

        return cusLt;
    }
    private void insertStayOnChainTKolsRptDay(List<TKolsRptDay> tKolsRptDays, String date) throws Exception {
        log.info("insertStayOnChainTKolsRptDay tradeDate:{} size:{}",date, tKolsRptDays.size());
        if (tKolsRptDays.size() >0) {
            String deleteSql = "delete from dc_kols_rpt_day where trade_date=? and status <> '1'";
            ExecTablePo deExecTablePo = new ExecTablePo();
            deExecTablePo.sql = deleteSql;
            deExecTablePo.args = new Object[]{date};
            deExecTablePo.type = ExceType.delete;

            ExecTablePo<TKolsRptDay> insert = new ExecTablePo();
            insert.tableName = "dc_kols_rpt_day";
            insert.insertlist = tKolsRptDays;
            insert.type = ExceType.insertList;
            List<ExecTablePo> execTablePoList = new ArrayList<>();
            execTablePoList.add(deExecTablePo);
            execTablePoList.add(insert);
            int num = tranSql(execTablePoList);
            log.info("insertStayOnChainTKolsRptDay tradeDate:{} size:{} num:{}", date, tKolsRptDays.size(), num);
        }
    }


    public void monTKolsJob(String date,List<TKols> kolsList) throws Exception {
        HashMap<String,TKolsRptDay> sumTKolsRpt = getSumTKolsRpt(date);
        String defaultDate = DateUtil.getDefaultDate();
        List<Object[]> list = new ArrayList<>();
        for (TKols tKols : kolsList){
            if (sumTKolsRpt.containsKey(tKols.kol_user_id)) {
                TKolsRptDay day = sumTKolsRpt.get(tKols.kol_user_id);
                list.add(new Object[]{ day.volume,day.invited_count,defaultDate,tKols.kol_user_id});

            }
        }
        if (list.size() >0) {
            String sql = "update dc_kols set volume=?,invited_count=?,update_time=? where kol_user_id=?";
            DBUtils.updateList(sql, list);
        }
        log.info("monTKolsJob tradeDate:{} size:{} ",date,list.size());
    }

    private HashMap<String,TKolsRptDay> getSumTKolsRpt(String date) throws Exception {
        String selectCus = "SELECT kol_user_id,sum(invited_count) as invited_count,sum(volume) as volume FROM dc_kols_rpt_day group by kol_user_id";
        List<TKolsRptDay> cusLt = DBUtils.queryListThrowsException(selectCus, new Object[]{}, TKolsRptDay.class);
        log.info("getSumTKolsRpt date:{} size:{}", date,cusLt.size());
        HashMap<String,TKolsRptDay> hm = new HashMap<>();
        for (TKolsRptDay cusRptDay : cusLt) {
            hm.put(cusRptDay.kol_user_id, cusRptDay);
        }
        log.info("getSumTKolsRpt date:{} size:{}", date,hm.size());
        return hm;
    }

    public void putKolsRptDayToChain(String date,List<TKolsRptDay> kolsList) throws Exception {

        if (batchStart.isRakeBackToChainEnabled()) {
            List<Object[]> list = new ArrayList<>();
            String symbolCategory = batchStart.dataFacade.getAllTSymbolCategory().get(0);
            log.info("putKolsRptDayToChain date:{} symbolCategory:{}", date, symbolCategory);
            for (TKolsRptDay day : kolsList) {
                try {
                    if (day.fee_amount != null && day.fee_amount.compareTo(BigDecimal.ZERO) > 0) {
                        LiquidationParam liquidationParam = new LiquidationParam();
                        String defaultDate = DateUtil.getDefaultDate();
                        liquidationParam.userAuthority = day.kol_user_id;
                        liquidationParam.amount = day.fee_amount;
                        String serverName = "Trade" + symbolCategory + "Svr";
                        String s = batchStart.tradeSvrClient.updateUSDCSnapshot(serverName, liquidationParam);
                        PResult pResult = JsonUtils.Deserialize(s, PResult.class);
                        day.status = ToChainStatus.Fail.getValue();
                        if (pResult != null) {
                            log.info(pResult.toString());
                            if (pResult.code == 0) {
                                day.status = ToChainStatus.Success.getValue();
                                day.txid = pResult.info1;
                                log.info(" kol_user_id:{} putKolsRptDayToChain success", day.kol_user_id);
                            }
                        }
                        list.add(new Object[]{day.status, day.txid, defaultDate, day.kol_user_id, date});
                    }
                } catch (Exception e) {
                    log.error("putKolsRptDayToChain", e);
                }
            }
            if (list.size() >0) {
                String sql = "update dc_kols_rpt_day set status=?,txid=?,update_time=? where kol_user_id=? and trade_date=?";
                DBUtils.updateList(sql, list);
            }
            log.info("update dc_kols_rpt_day size:{}", list.size());
        }
    }


}
