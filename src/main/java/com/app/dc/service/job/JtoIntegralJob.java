package com.app.dc.service.job;

import com.app.common.db.DBUtils;
import com.app.common.utils.JsonUtils;
import com.app.common.utils.StringUtil;
import com.app.dc.data.Symbol;
import com.app.dc.entity.*;
import com.app.dc.enums.ExceType;
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
public class JtoIntegralJob extends TranSQL implements Job {

    @Setter
    private BatchStart batchStart;

    private static boolean runFlag = false;
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        log.info("JtoIntegralJob execute");
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
                log.info("JtoIntegralJob Job start");
                calJto();
                log.info("JtoIntegralJob Job end");
            } catch (Exception e) {
                log.error("JtoIntegralJob error", e);
            }finally {
                runFlag = false;
            }
        } else {
            log.warn("JtoIntegralJob job is runing.");

        }
    }
    private void calJto() throws Exception {
        String tradeDate = getDate();
        log.info("calJto JtoStartDate:{} JtoEndDate:{} tradeDate:{} JtoAmount:{}",batchStart.getJtoStartDate(), batchStart.getJtoEndDate(),tradeDate,batchStart.getJtoAmount());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date1 = sdf.parse(batchStart.getJtoStartDate());
        Date date2 = sdf.parse(batchStart.getJtoEndDate());

        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH");
        Date toadDate = sdf1.parse(tradeDate);
        if (toadDate.getTime() >= date1.getTime() && toadDate.getTime() <= date2.getTime()) {

        }else {
            log.warn("calJto The date is not within the range, tradeDate:{}",tradeDate);
            return;
        }
        List<Symbol> list = batchStart.dataFacade.getJitoSOLSymbolList(batchStart.getJtoStartDate(), batchStart.getJtoEndDate());
        if (list.size() == 0){
            log.warn("calJto Symbol size=0, tradeDate:{}",tradeDate);
            return;
        }
        List<String> symbols = new ArrayList<>();
        for (Symbol symbol : list) {
            symbols.add(symbol.getSymbol());
        }
        Map<String,TSolanaLpJtoFee> feeMap = new HashMap<>();
        List<TSolanaCollectFeeRecord> collectFeeRecords = getSumCollectFeeRecord(symbols);
        for (TSolanaCollectFeeRecord record : collectFeeRecords){
            if (StringUtil.isNotEmpty(record.security_id) && symbols.contains(record.security_id)){
                String key = String.format("%s_%s",record.user_id,record.user);
                BigDecimal fee_amount = record.fee_amount;
                if(fee_amount !=null) {
                    if (feeMap.containsKey(key)) {
                        TSolanaLpJtoFee fee = feeMap.get(key);
                        fee.cllect_fee = fee.cllect_fee.add(fee_amount);
                    }else {
                        TSolanaLpJtoFee fee = new TSolanaLpJtoFee();
                        fee.cllect_fee = fee_amount;
                        fee.user_id = record.user_id;
                        fee.lp = record.user;
                        fee.symbol = record.security_id;
                        feeMap.put(key, fee);
                    }
                }
            }
        }
        List<String> jitoSolLPs = getLp(symbols);
        getLPClaimFee(Consts.CATEGORY_SOL,symbols,jitoSolLPs, feeMap);
        String defaultDate = DateUtil.getDefaultDate();
        Set<String> sets = feeMap.keySet();
        for (String key : sets) {
            TSolanaLpJtoFee fee = feeMap.get(key);
            fee.trade_date = tradeDate;
            fee.create_time = defaultDate;
        }

        List<TSolanaLpJtoFee> JtoFeeList = new ArrayList<>();
        JtoFeeList.addAll(feeMap.values());
        int number = updateTSolanaLpJtoFee(JtoFeeList,tradeDate);
        if (number > 0) {
            List<TSolanaLpJtoFee> sumJtoFee = getSumJtoFee(tradeDate);
            BigDecimal total_collect_claim_fee = null;
            Map<String,BigDecimal> userSumFee = new HashMap<>();
            for (TSolanaLpJtoFee fee : sumJtoFee){
                total_collect_claim_fee = DataUtils.getAdd(total_collect_claim_fee,fee.cllect_fee);
                total_collect_claim_fee = DataUtils.getAdd(total_collect_claim_fee,fee.claim_fee);
                BigDecimal userFee = null;
                if (userSumFee.containsKey(fee.user_id)){
                    userFee = userSumFee.get(fee.user_id);
                }
                userFee = DataUtils.getAdd(userFee,fee.cllect_fee);
                userFee = DataUtils.getAdd(userFee,fee.claim_fee);
                if (userFee != null) {
                    userSumFee.put(fee.user_id, userFee);
                }
            }

            if (total_collect_claim_fee != null && total_collect_claim_fee.compareTo(BigDecimal.ZERO) != 0) {
                List<TSolanaLpJto> jtolist = new ArrayList<>();
                for (TSolanaLpJtoFee fee : sumJtoFee) {
                    if (userSumFee.containsKey(fee.user_id)) {
                        TSolanaLpJto lpJto = new TSolanaLpJto();
                        lpJto.user_id = fee.user_id;
                        lpJto.create_time = defaultDate;
                        lpJto.user_collect_claim_fee = userSumFee.get(fee.user_id);
                        lpJto.total_collect_claim_fee = total_collect_claim_fee;
                        lpJto.jto_total_supply = batchStart.getJtoAmount();
                        BigDecimal jto = lpJto.user_collect_claim_fee.divide(total_collect_claim_fee, MathContext.DECIMAL128).multiply(batchStart.getJtoAmount());
                        jto = jto.setScale(9, RoundingMode.DOWN);
                        lpJto.jto = jto;
                        log.info("calJto user_id:{} jito:{} total_collect_claim_fee:{} JtoAmount:{}",lpJto.user_id ,jto,total_collect_claim_fee,batchStart.getJtoAmount());
                        jtolist.add(lpJto);
                    }
                }
                updateTSolanaLpJto(jtolist);
            }else {
                log.warn("total_collect_claim_fee error,total_collect_claim_fee:{}",total_collect_claim_fee);
            }

        }
    }

    public String getDate() throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH");
        String date = format.format(new Date());
        return date;
    }

    private List<TSolanaCollectFeeRecord> getSumCollectFeeRecord(List<String> symbols) throws Exception {
        String sql ="SELECT r.* FROM dc_solana_collect_fee_record r inner join dc_solana_lp_users d on r.user_id = d.lp_authority and r.user = d.lp ";
        List<TSolanaCollectFeeRecord> list = DBUtils.queryListThrowsException(sql,null,TSolanaCollectFeeRecord.class);
        log.info("getSumCollectFeeRecord size:{}",list.size());
        List<TSolanaCollectFeeRecord> result = new ArrayList<>();
        for (TSolanaCollectFeeRecord record : list){
            if (StringUtil.isNotEmpty(record.security_id) && symbols.contains(record.security_id)){
                result.add(record);
            }
        }
        log.info("getSumCollectFeeRecord result size:{}",result.size());
        return result;
    }

    public List<String> getLp(List<String> symbols) {
        String sql = "SELECT *  FROM dc_solana_lp_users where is_active=1 and is_delete != '1' and market_indicator='SOL'";
        log.info("getLp sql:{}", sql);
        List<String> lps = new ArrayList<>();
        List<TSolanaLpUsers> list = DBUtils.queryList(sql,  new Object[] {}, TSolanaLpUsers.class);
        for (TSolanaLpUsers tSolanaLpUsers : list) {
            if (StringUtil.isNotEmpty(tSolanaLpUsers.security_id) && symbols.contains(tSolanaLpUsers.security_id)) {
                lps.add(tSolanaLpUsers.lp);
            }
        }
        log.info("getLp size:{}", lps.size());
        return lps;
    }

    public void getLPClaimFee(String symbolCategory, List<String> symbols, List<String> lps,Map<String,TSolanaLpJtoFee> feeMap) {
        List<String> req = new ArrayList<>();
        int count = 40;
        for (String lp : lps) {
            if (req.size() >= count) {
                getTradeLPClaimFee(symbolCategory,symbols,req,feeMap);
                req.clear();
            } else {
                req.add(lp);
            }
        }
        if (req.size() > 0) {
            getTradeLPClaimFee(symbolCategory,symbols,req,feeMap);
            req.clear();
        }
    }

    private void getTradeLPClaimFee(String symbolCategory,List<String> symbols,List<String> req,Map<String,TSolanaLpJtoFee> feeMap) {
        String content = batchStart.tradeSvrClient.getLPClaimFee("Trade"+symbolCategory+"Svr",req);
        LPValueResult lpValueResult = JsonUtils.Deserialize(content, LPValueResult.class);
        if (lpValueResult != null && lpValueResult.data != null) {
            for (LPValue lpValue : lpValueResult.data) {
                if (StringUtil.isNotEmpty(lpValue.securityId) && symbols.contains(lpValue.securityId)) {
                    if (lpValue.claim_fee != null) {
                        BigDecimal claim_fee = lpValue.claim_fee;
                        String key = String.format("%s_%s",lpValue.user_id,lpValue.lp);
                        if (feeMap.containsKey(key)) {
                            TSolanaLpJtoFee po = feeMap.get(key);
                            po.market_index = lpValue.marketIndex;
                            po.perp_market = lpValue.perpMarket;
                            po.claim_fee = DataUtils.getAdd(po.claim_fee, claim_fee);
                        }else {
                            TSolanaLpJtoFee fee = new TSolanaLpJtoFee();
                            fee.claim_fee = claim_fee;
                            fee.user_id = lpValue.user_id;
                            fee.lp = lpValue.lp;
                            fee.market_index = lpValue.marketIndex;
                            fee.perp_market = lpValue.perpMarket;
                            fee.symbol = lpValue.securityId;
                            feeMap.put(key, fee);
                        }

                    }
                }
            }
        }
    }

    private int updateTSolanaLpJtoFee(List<TSolanaLpJtoFee> jtoFees,
                                               String tradeDate) throws Exception {
        String sql = "delete from dc_solana_lp_jto_fee where trade_date=?";
        ExecTablePo deExecTablePo = new ExecTablePo();
        deExecTablePo.sql = sql;
        deExecTablePo.args = new Object[] { tradeDate };
        deExecTablePo.type = ExceType.delete;

        ExecTablePo<TSolanaLpJtoFee> insert = new ExecTablePo();
        insert.tableName ="dc_solana_lp_jto_fee";
        insert.insertlist = jtoFees;
        insert.type = ExceType.insertList;

        List<ExecTablePo> execTablePoList = new ArrayList<>();
        execTablePoList.add(deExecTablePo);
        execTablePoList.add(insert);
        int num = tranSql(execTablePoList);
        log.info("updateTSolanaLpJtoFee tradeDate:{} size:{} tranSql number:{}", tradeDate, jtoFees.size(),num);
        return num;
    }

    private List<TSolanaLpJtoFee> getSumJtoFee(String trade_date) throws Exception {
        String sql = "SELECT user_id,sum(cllect_fee) as cllect_fee,sum(claim_fee) as claim_fee  FROM dc_solana_lp_jto_fee where trade_date=? and user_id not in(select user_id from dc_solana_lp_jto_black_list) group by user_id";
        List<TSolanaLpJtoFee> cusLt = DBUtils.queryListThrowsException(sql, new Object[]{trade_date}, TSolanaLpJtoFee.class);
        log.info("getSumJtoFee trade_date:{},size:{}",trade_date, cusLt.size());
        return cusLt;
    }

    private int updateTSolanaLpJto(List<TSolanaLpJto> jtos) throws Exception {
        String sql = "delete from dc_solana_lp_jto";
        ExecTablePo deExecTablePo = new ExecTablePo();
        deExecTablePo.sql = sql;
        deExecTablePo.args = new Object[] { };
        deExecTablePo.type = ExceType.delete;

        ExecTablePo<TSolanaLpJto> insert = new ExecTablePo();
        insert.tableName ="dc_solana_lp_jto";
        insert.insertlist = jtos;
        insert.type = ExceType.insertList;

        List<ExecTablePo> execTablePoList = new ArrayList<>();
        execTablePoList.add(deExecTablePo);
        execTablePoList.add(insert);
        int num = tranSql(execTablePoList);
        log.info("updateTSolanaLpJto size:{} tranSql number:{}", jtos.size(),num);
        return num;
    }
}
