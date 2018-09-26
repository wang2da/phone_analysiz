package com.phone.etl.mr.session;

import com.phone.etl.analysis.dim.base.BaseDimension;
import com.phone.etl.analysis.dim.base.StatsUserDimension;
import com.phone.etl.common.GloadUtils;
import com.phone.etl.mr.IOWriterOutput;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.output.BaseOutput;
import com.phone.etl.output.reduce.ReduceOutput;
import com.phone.etl.utils.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.sql.Date;
import java.sql.PreparedStatement;

public class SessionOutputWritter implements IOWriterOutput {
    @Override
    public void output(Configuration conf, BaseDimension k, BaseOutput v, PreparedStatement ps, IDimension iDimension) throws Exception {
        try{
            StatsUserDimension key = (StatsUserDimension)k;
            ReduceOutput value = (ReduceOutput)v;

            int sessions = ((IntWritable) value.getValue().get(new IntWritable(-1))).get();
            int sessionLength = ((IntWritable) value.getValue().get(new IntWritable(-2))).get();

            int i = 0;

            ps.setInt(++i,iDimension.getDimensionByObject((key.getStatsCommonDismension().getDateDimension())));
            ps.setInt(++i,iDimension.getDimensionByObject(key.getStatsCommonDismension().getPlatformDimension()));
            ps.setInt(++i,sessions);
            ps.setInt(++i,sessionLength);
            ps.setString(++i,conf.get(GloadUtils.RUNNING_DATE));
            ps.setInt(++i,sessions);
            ps.setInt(++i,sessionLength);

            ps.addBatch();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
