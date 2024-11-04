package com.app.dc.service.job;

import com.app.common.db.DBUtils;
import com.app.common.utils.StringUtil;
import com.app.dc.entity.ExecTablePo;
import com.app.dc.entity.TSolanaCollectFeeRecord;
import com.app.dc.entity.TSolanaLpApr;
import com.app.dc.entity.TSolanaLpRecord;
import com.app.dc.enums.ExceType;
import com.app.dc.service.BatchStart;
import com.app.dc.util.Consts;
import com.app.dc.utils.DateUtil;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.math.BigDecimal;
import java.math.MathContext;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class SolanaLpAprJob extends TranSQL implements Job {


    private static boolean runFlag = false;

    @Setter
    private BatchStart batchStart;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        log.info("SolanaLpAprJob execute");
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
                log.info("SolanaLpAprJob Job start");
                calLpApr();
                log.info("SolanaLpAprJob Job end");
            } catch (Exception e) {
                log.error("SolanaLpAprJob error", e);
            }finally {
                runFlag = false;
            }
        } else {
            log.warn("SolanaLpAprJob job is runing.");

        }
    }

    private void calLpApr() throws Exception {
        String endDate = getEndDate();
        log.info("calLpApr endDate:{}",endDate);
        if (StringUtil.isNotEmpty(endDate)){
            long ts_apr1 = new Date().getTime();
            long ts_apr = ts_apr1 / 1000;
            log.info("calLpApr ts_apr1:{} ts_apr:{}",ts_apr1,ts_apr);
            BigDecimal ts_apr_big = new BigDecimal(String.valueOf(ts_apr));
            List<TSolanaLpApr> endDateSolanaLpApr = getTSolanaLpApr(endDate);
            if (endDateSolanaLpApr == null || endDateSolanaLpApr.size() == 0){
                HashMap<String, List<TSolanaLpRecord>> lpRecordHm = getTSolanaLpRecordHm();
                HashMap<String, BigDecimal> feeHm = getSumCollectFeeRecord();
                Set<Map.Entry<String, List<TSolanaLpRecord>>> entrySet =  lpRecordHm.entrySet();
                String defaultDate = DateUtil.getDefaultDate();
                List<TSolanaLpApr> aprs = new ArrayList<>();
                List<String> aprKeys = new ArrayList<>();
                for (Map.Entry<String, List<TSolanaLpRecord>> entry : entrySet) {
                    String key = entry.getKey();
                    List<TSolanaLpRecord> lpRecord = entry.getValue();
                    solanaLpRecordTsAsc(lpRecord);
                    TSolanaLpRecord record0 = lpRecord.get(0);
                    long holding_period = 0;
                    if (record0 != null && StringUtil.isNotEmpty(record0.ts)){
                        long minTs = Long.parseLong(record0.ts);
                        holding_period = ts_apr - minTs;
                        log.info("holding_period user_id:{} account_id:{} symbol:{} holding_period:{} ts:{}",record0.user_id,record0.account_id,record0.symbol,holding_period,record0.ts);
                    }
                    BigDecimal avg_quote_amount = null;
                    for (TSolanaLpRecord record : lpRecord){
                        if (StringUtil.isNotEmpty(record.ts) && holding_period != 0){
                            BigDecimal ts = new BigDecimal(record.ts);
                            BigDecimal holding = new BigDecimal(holding_period);
                            BigDecimal time_factor = ts_apr_big.subtract(ts).divide(holding, MathContext.DECIMAL128);
                            log.info("holding_period user_id:{} account_id:{} symbol:{} holding_period:{} ts:{} time_factor:{}",record.user_id,record.account_id,record.symbol,holding_period,record.ts,time_factor);
                            if (record.minted_quote_amount != null) {
                                BigDecimal quote_amount = record.minted_quote_amount.multiply(time_factor);
                                if (avg_quote_amount == null) {
                                    avg_quote_amount = quote_amount;
                                } else {
                                    avg_quote_amount = avg_quote_amount.add(quote_amount);
                                }
                            }
                        }
                    }

                    BigDecimal fee = feeHm.get(key);
                    for (TSolanaLpRecord record : lpRecord){

                        String aprKey= record.user_id+record.account_id+record.symbol;
                        if (!aprKeys.contains(aprKey)) {
                            aprKeys.add(aprKey);
                            TSolanaLpApr apr = new TSolanaLpApr();
                            apr.user_id = record.user_id;
                            apr.account_id = record.account_id;
                            apr.collected_fee = fee;
                            apr.symbol = record.symbol;
                            apr.holding_period = holding_period + "";
                            apr.trade_date = endDate;
                            apr.update_time = defaultDate;
                            apr.market_indicator = record.market_indicator;
                            apr.avg_quote_amount = avg_quote_amount;
                            aprs.add(apr);
                        }
                    }
                }
                insertTSolanaLpApr(aprs,endDate);
            }
        }else {
            log.error("endDate is null");
        }
    }

    public String getEndDate() throws ParseException {
        Date date = getDateH();
        String[] dateInt = Consts.hourlyInterval;
        int len = dateInt.length;
        String tradeStr = getDate();
        for (int i =0; i<len; i++) {
            String trade = String.format("%s %s",tradeStr,dateInt[i]);
            Date date1 = parseDate(trade);
            if (date1.getTime() == date.getTime()){
                String dateStr = formatDate(date);
                return dateStr;
            }else{
                if (date.getTime() > date1.getTime()){
                    int i1 = i+1;
                    if (i1 < len){
                        String trade1 = String.format("%s %s",tradeStr,dateInt[i1]);
                        Date date2 = parseDate(trade1);
                        if (date.getTime() < date2.getTime()){
                            String dateStr = formatDate(date1);
                            return dateStr;
                        }
                    }else if(i == len-1){
                        String dateStr = formatDate(date1);
                        return dateStr;
                    }
                }
            }
        }

        return null;
    }

    private static String getLastDateH(String dateStr) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH");
        Date date1 = format.parse(dateStr);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date1);
        cal.add(Calendar.HOUR_OF_DAY, -6);
        String date = format.format(cal.getTime());
        return date;
    }


    public static String formatDate(Date date) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH");
        String dateStr = format.format(date);
        return dateStr;
    }

    public static Date parseDate(String dateStr) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH");
        Date date = format.parse(dateStr);
        return date;
    }

    public static Date getDateH() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH");
        Calendar cal = Calendar.getInstance();
//        try {
//            Date date1 = format.parse("2024-07-24 01");
//            cal.setTime(date1);
//        } catch (ParseException e) {
//            throw new RuntimeException(e);
//        }
        return cal.getTime();
    }

    public static String getDate() {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String date;
        Calendar cal = Calendar.getInstance();
        date = format.format(cal.getTime());
        return date;
    }



    private HashMap<String, List<TSolanaLpRecord>> getTSolanaLpRecordHm() throws Exception {
        String sql ="select r.* from dc_solana_lp_record r inner join dc_solana_lp_users d on r.user_id = d.lp_authority and r.account_id = d.lp and r.is_active = 1 and d.is_active = 1 and d.is_delete <> '1'  and r.type='0'";
        List<TSolanaLpRecord> list = DBUtils.queryListThrowsException(sql,null,TSolanaLpRecord.class);
        log.info("getTSolanaLpRecord size:{}",list.size());

        HashMap<String, List<TSolanaLpRecord>> lpRecordHm = new HashMap<>();
        for (TSolanaLpRecord record : list) {
            String key = String.format("%s_%s", record.user_id, record.account_id);
            List<TSolanaLpRecord> lpRecords = lpRecordHm.get(key);
            if (lpRecords == null){
                lpRecords = new ArrayList<>();
                lpRecordHm.put(key,lpRecords);
            }
            lpRecords.add(record);
        }
        log.info("getTSolanaLpRecord recordHm size:{}",lpRecordHm.size());
        return lpRecordHm;
    }



    private HashMap<String, BigDecimal> getSumCollectFeeRecord() throws Exception {
        String sql ="SELECT r.* FROM dc_solana_collect_fee_record r inner join dc_solana_lp_users d on r.user_id = d.lp_authority and r.user = d.lp and  d.is_active = 1 and d.is_delete <> '1'";
        List<TSolanaCollectFeeRecord> list = DBUtils.queryListThrowsException(sql,null,TSolanaCollectFeeRecord.class);
        log.info("getSumCollectFeeRecord size:{}",list.size());
        HashMap<String, BigDecimal> feeHm = new HashMap<>();
        for (TSolanaCollectFeeRecord record : list){
            String key = String.format("%s_%s",record.user_id,record.user);
            BigDecimal fee = record.fee_amount;
            if(fee !=null) {
                if (feeHm.containsKey(key)) {
                    BigDecimal sumFee = feeHm.get(key);
                    fee = fee.add(sumFee);
                }
                feeHm.put(key,fee);
            }
        }
        log.info("getSumCollectFeeRecord feeHm size:{}",feeHm.size());
        return feeHm;
    }

    private List<TSolanaLpApr> getTSolanaLpApr(String endDate) throws Exception {
        String sql ="SELECT * FROM dc_solana_lp_apr where trade_date=?";
        List<TSolanaLpApr> list = DBUtils.queryListThrowsException(sql,new Object[]{endDate},TSolanaLpApr.class);
        log.info("getTSolanaLpApr trade_date:{} size:{}",endDate,list.size());
        return list;
    }

    private void insertTSolanaLpApr(List<TSolanaLpApr> list,String trade_date) throws Exception {
//        String sql ="delete from dc_solana_lp_apr where trade_date=?";
//        DBUtils.update(sql,new Object[]{trade_date});
//        DBUtils.insertList(list,"dc_solana_lp_apr");
        if (list.size() >0) {
            String sql = "delete from dc_solana_lp_apr where trade_date=?";
            ExecTablePo deExecTablePo = new ExecTablePo();
            deExecTablePo.sql = sql;
            deExecTablePo.args = new Object[]{trade_date};
            deExecTablePo.type = ExceType.delete;

            ExecTablePo<TSolanaLpApr> insert = new ExecTablePo();
            insert.tableName = "dc_solana_lp_apr";
            insert.insertlist = list;
            insert.type = ExceType.insertList;

            List<ExecTablePo> execTablePoList = new ArrayList<>();
            execTablePoList.add(deExecTablePo);
            execTablePoList.add(insert);
            int num = tranSql(execTablePoList);
            log.info("insertTSolanaLpApr trade_date:{} size:{} number:{}", trade_date, list.size(), num);
        }
    }

    private void solanaLpRecordTsAsc(List<TSolanaLpRecord> list){
        Collections.sort(list, Comparator.comparing(TSolanaLpRecord::getTs, Comparator.nullsFirst(Comparator.naturalOrder())));
    }
}
