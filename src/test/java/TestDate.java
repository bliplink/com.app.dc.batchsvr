import com.app.dc.entity.TSolanaHFeeEx;
import com.app.dc.service.job.SolanaLpAprJob;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestDate {

    @Test
    public void testLongDate(){
        long l = 1721630922000l;
        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(l);
        String formattedDateTime = format1.format(date);
        System.out.println(formattedDateTime);
    }


    @Test
    public void testEndDate(){
        SolanaLpAprJob solanaLpAprJob = new SolanaLpAprJob();
        try {
            String endDate = solanaLpAprJob.getEndDate();
            System.out.println(endDate);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDateLong() {
        long ts_apr = new Date().getTime();
        System.out.println(ts_apr);
        ts_apr = ts_apr / 1000;
        System.out.println(ts_apr);
    }

    @Test
    public void testHourlyInterval(){
        int hourlyInterval = 4;
        int hourly24 = 24;
        int count = hourly24 / hourlyInterval;
        String[] hourlys = new String[count];
        for (int i =0; i < count; i++){
            int begin =i * hourlyInterval;
            String hourly = String.valueOf(begin);
            if (hourly.length() == 1){
                hourly ="0"+hourly;
            }
            hourlys[i]=hourly;
            System.out.println(hourly);
        }
        System.out.println(hourlys);
    }

    @Test
    public void tetsBig(){
        int day =1;
        TSolanaHFeeEx ex = new TSolanaHFeeEx();
        ex.count = 24;
        int count = day * 24;
        BigDecimal m = new BigDecimal(day+"");
        if (count > ex.count){
            m = new BigDecimal(String.valueOf(ex.count)).divide(new BigDecimal(String.valueOf(24)),MathContext.DECIMAL128).setScale(2,RoundingMode.DOWN);
        }
        System.out.println(m);
    }
}
