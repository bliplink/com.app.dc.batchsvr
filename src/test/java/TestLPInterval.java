import com.app.dc.entity.TSymbolLpInterval;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

public class TestLPInterval {

    @Test
    public void testHourlyInterval(){
        BigDecimal yield = new BigDecimal("0.033021987");
        BigDecimal interval = new BigDecimal("0.01");
        BigDecimal percentage = new BigDecimal("100");
        String[] LPIntervalMultiples = new String[] {"1.5","2.5"};
        BigDecimal yield100 = yield.multiply(percentage);
        BigDecimal interval100 = interval.multiply(percentage);
        BigDecimal divideYield = yield100.divide(interval100, MathContext.DECIMAL128);
        BigDecimal lower = divideYield.setScale(0, RoundingMode.FLOOR).divide(interval100);
        BigDecimal upper = divideYield.setScale(0, RoundingMode.CEILING).divide(interval100);
        System.out.println("lower:"+lower+" upper:"+upper);
        for (String recommendIntervals : LPIntervalMultiples){
            TSymbolLpInterval tSymbolLpInterval = new TSymbolLpInterval();
            BigDecimal b = new BigDecimal(recommendIntervals);
            BigDecimal recommendLower = lower.divide(b, MathContext.DECIMAL128).divide(interval, MathContext.DECIMAL128).setScale(0, RoundingMode.FLOOR).multiply(interval);
            BigDecimal recommendUpper = upper.add(lower.subtract(recommendLower));
//            tSymbolLpInterval.symbol = marketPrice.getSecurityId();
            tSymbolLpInterval.multiples = b;
            tSymbolLpInterval.yield = yield;
            tSymbolLpInterval.lower = recommendLower.divide(percentage);
            tSymbolLpInterval.upper = recommendUpper.divide(percentage);
            System.out.println(tSymbolLpInterval);
//            tSymbolLpInterval.create_time = defaultDate;
        }

    }
}
