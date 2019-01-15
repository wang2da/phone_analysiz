package com.phone.etl.hive.en;

import com.phone.etl.analysis.dim.base.DateDimension;
import com.phone.etl.analysis.dim.base.DateEnum;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.mr.ua.service.impl.IDimensionImpl;
import com.phone.etl.utils.TimeUtil;
import jodd.util.StringUtil;
import org.apache.hadoop.hive.ql.exec.UDF;

public class DateDimensionUDF extends UDF {

    IDimension iDimension = new IDimensionImpl();

    public int evaluate(String date){
        if(StringUtil.isEmpty(date)){
            date = TimeUtil.getYesterdayDate();
        }
        DateDimension dateDimension = DateDimension.buildDate(TimeUtil.parserString2Long(date), DateEnum.DAY);
        int id = 0;
        try {
            id = iDimension.getDimensionByObject(dateDimension);
        }catch (Exception e){
            e.printStackTrace();
        }
        return id;
    }


    public static void main(String[] args) {
        System.out.println(new DateDimensionUDF().evaluate("2018-09-24"));
    }
}
