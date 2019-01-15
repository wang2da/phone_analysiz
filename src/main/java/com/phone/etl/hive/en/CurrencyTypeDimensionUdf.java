package com.phone.etl.hive.en;


import com.phone.etl.analysis.dim.base.CurrencyTypeDimension;
import com.phone.etl.common.GloadUtils;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.mr.ua.service.impl.IDimensionImpl;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 获取货币类型维度的Id
 * Created by lyd on 2018/4/9.
 */
public class CurrencyTypeDimensionUdf extends UDF{

    public IDimension converter =null;

    public CurrencyTypeDimensionUdf(){
        converter = new IDimensionImpl();
    }

    /**
     *
     * @param name
     * @return
     */
    public int evaluate(String name){
        name = name == null || StringUtils.isEmpty(name.trim()) ? GloadUtils.DEFAULT_VALUE :name.trim() ;
        CurrencyTypeDimension currencyTypeDimension = new CurrencyTypeDimension(name);
        try {
            return converter.getDimensionByObject(currencyTypeDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new RuntimeException("获取货币类型维度的Id异常");
    }

    public static void main(String[] args) {
        System.out.println(new CurrencyTypeDimensionUdf().evaluate(null));
    }
}
