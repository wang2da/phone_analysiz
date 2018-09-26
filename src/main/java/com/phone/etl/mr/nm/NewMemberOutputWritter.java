package com.phone.etl.mr.nm;

import com.phone.etl.analysis.dim.base.BaseDimension;
import com.phone.etl.analysis.dim.base.StatsUserDimension;
import com.phone.etl.common.GloadUtils;
import com.phone.etl.mr.IOWriterOutput;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.output.BaseOutput;
import com.phone.etl.output.map.MapOutput;
import com.phone.etl.output.reduce.ReduceOutput;
import com.phone.etl.utils.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import java.sql.Date;
import java.sql.PreparedStatement;

public class NewMemberOutputWritter implements IOWriterOutput {
    @Override
    public void output(Configuration conf, BaseDimension k, BaseOutput v, PreparedStatement ps, IDimension iDimension) throws Exception {
        try{
            StatsUserDimension key = (StatsUserDimension)k;
            ReduceOutput value = (ReduceOutput)v;

            int i = 0;

            switch (v.getKpi()){
                case NEW_MEMBER:
                case BROWSER_NEW_MEMBER:
                    int newUser = ((IntWritable) value.getValue().get(new IntWritable(-1))).get();
                    ps.setInt(++i,iDimension.getDimensionByObject((key.getStatsCommonDismension().getDateDimension())));
                    ps.setInt(++i,iDimension.getDimensionByObject(key.getStatsCommonDismension().getPlatformDimension()));
                    ps.setInt(++i,newUser);
                    ps.setString(++i,conf.get(GloadUtils.RUNNING_DATE));
                    ps.setInt(++i,newUser);
                    break;
                case MEMBER_INFO:
                    String memberId = ((Text)(value.getValue().get(new IntWritable(-2)))).toString();
                    ps.setString(++i,memberId);
                    ps.setDate(++i,new Date(TimeUtil.parserString2Long(conf.get(GloadUtils.RUNNING_DATE))));
                    ps.setLong(++i,TimeUtil.parserString2Long(conf.get(GloadUtils.RUNNING_DATE)));;
                    ps.setDate(++i,new Date(TimeUtil.parserString2Long(conf.get(GloadUtils.RUNNING_DATE))));
                    ps.setDate(++i,new Date(TimeUtil.parserString2Long(conf.get(GloadUtils.RUNNING_DATE))));
                    break;
                default:break;
            }

            ps.addBatch();

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
