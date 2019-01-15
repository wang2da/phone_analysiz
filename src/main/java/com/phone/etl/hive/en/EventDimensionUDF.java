package com.phone.etl.hive.en;

import com.phone.etl.analysis.dim.base.EventDimension;
import com.phone.etl.common.GloadUtils;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.mr.ua.service.impl.IDimensionImpl;
import jodd.util.StringUtil;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

public class EventDimensionUDF extends UDF {
    IDimension iDimension = new IDimensionImpl();

    public int evaluate(String category,String action){
        if(StringUtil.isEmpty(category)){
            category=action= GloadUtils.DEFAULT_VALUE;
        }
        if(StringUtil.isEmpty(action)){
            action=GloadUtils.DEFAULT_VALUE;
        }
        int id = -1;

        try {
            EventDimension ed = new EventDimension(category,action);
            id= iDimension.getDimensionByObject(ed);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public static void main(String[] args) {
        System.out.println(new EventDimensionUDF().evaluate("aa","bb") );
    }

}
