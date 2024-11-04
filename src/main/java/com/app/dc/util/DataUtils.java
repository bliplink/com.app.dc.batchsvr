package com.app.dc.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class DataUtils {

	
	public static String listToString(List<String> lt) {
		String str = "";
		for (String string : lt) {
			str+="'"+string+"',";
		}
		if(str.length()>0) {
			str=str.substring(0,str.length()-1);
		}
		return str;
	}
	
    /**
     * Get all dates within a certain period of time
     * @param startDateStr yyyy-MM-dd
     * @param endDateStr yyyy-MM-dd
     * @return yyyy-MM-dd
     */
    public static List<String> findDates(String startDateStr, String endDateStr) throws ParseException {
        Date startDate;
        Date endDate;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        startDate = sdf.parse(startDateStr);
        endDate = sdf.parse(endDateStr);
        Calendar cStart = Calendar.getInstance();
        cStart.setTime(startDate);

        List dateList = new ArrayList();
        //Add start date
        dateList.add(startDateStr);
        // Is this date after the specified date
        while (endDate.after(cStart.getTime())) {
            cStart.add(Calendar.DAY_OF_YEAR, 1);
            if(endDate.compareTo(cStart.getTime()) < 0) {
                break;
            }
            dateList.add(sdf.format(cStart.getTime()));
        }
        return dateList;
    }

    public static String[] getArray(String value, String spitCahr) {
        if (value != null) {
            ArrayList<String> list = new ArrayList<>();
            String[] values = value.split(getKey(spitCahr));
            for (String v : values) {
                if (isNotEmpty(v)) {
                    v = v.trim();
                    if (list.indexOf(v) < 0) {
                        list.add(v);
                    }
                }
            }
            if (list.size() > 0) {
                values = new String[list.size()];
                values = list.toArray(values);
                return values;
            }
        }
        return null;
    }


    public static List<String> getListStr(String value) {
        ArrayList<String> list = new ArrayList<>();
        if (value != null) {
            String[] values = value.split(getKey(","));
            for (String v : values) {
                if (isNotEmpty(v)) {
                    v = v.trim();
                    if (list.indexOf(v) < 0) {
                        list.add(v);
                    }
                }
            }

        }
        return list;
    }

    public static String getKey(String key){
        if(key != null && key.length() >0){
            if (key.equals("|") || key.equals("*") || key.equals("+") || key.equals(":") || key.equals(".")) {
                key = "\\"+key;
            }
        }
        return key;
    }

    public static boolean isNotEmpty(String str) {
        return ((str != null) && (str.trim().length() > 0));
    }

    public static String getString(double d) {
        String result = String.format("%.11f", d);
        BigDecimal bigDecimal = new BigDecimal(result);
        bigDecimal.setScale(9, RoundingMode.DOWN);
        return bigDecimal.toString();
    }

    public static BigDecimal getDecStr(String d) {
        try {
            if (d != null && d.length() > 0) {
                BigDecimal bigDecimal = new BigDecimal(d);
                return bigDecimal;
            }
        }catch (Exception e){
            throw new RuntimeException("convert BigDecimal error, str:"+d);
        }

        return null;
    }

    public static BigDecimal getAdd(BigDecimal b1, BigDecimal b2) {
        if (b1 == null && b2 == null) {
            return null;
        }
        if (b1 == null) {
            return b2;
        }else if (b2 == null){
            return b1;
        }
        BigDecimal b = b1.add(b2);
        return b;
    }



    public static BigDecimal getScaleDOWN(BigDecimal b) {
        if (b != null){
           b = b.setScale(9,RoundingMode.DOWN);
        }
        return b;
    }

}
