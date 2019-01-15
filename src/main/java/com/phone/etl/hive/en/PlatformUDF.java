package com.phone.etl.hive.en;

import com.phone.etl.analysis.dim.base.PlatformDimension;
import com.phone.etl.common.GloadUtils;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.mr.ua.service.impl.IDimensionImpl;
import jodd.util.StringUtil;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

public class PlatformUDF extends UDF {
    IDimension iDimension = new IDimensionImpl();

    public int evaluate(String platform){
        if(StringUtil.isEmpty(platform)){
            platform = GloadUtils.DEFAULT_VALUE;
        }
        int id = -1;
        try {
            PlatformDimension pl = new PlatformDimension(platform);
            id = iDimension.getDimensionByObject(pl);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }

    public static void main(String[] args) {
        System.out.println(new PlatformUDF().evaluate("website"));
    }
}
