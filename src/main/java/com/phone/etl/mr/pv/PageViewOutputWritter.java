package com.phone.etl.mr.pv;

import com.phone.etl.analysis.dim.base.BaseDimension;
import com.phone.etl.analysis.dim.base.StatsUserDimension;
import com.phone.etl.common.GloadUtils;
import com.phone.etl.mr.etl.EtlToHdfs;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.output.BaseOutput;
import com.phone.etl.output.reduce.ReduceOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.sql.PreparedStatement;

public class PageViewOutputWritter implements EtlToHdfs.IOWriterOutput {
    @Override
    public void output(Configuration conf, BaseDimension k, BaseOutput v, PreparedStatement ps, IDimension iDimension) throws Exception {
        try{


            StatsUserDimension key = (StatsUserDimension)k;
            ReduceOutput value = (ReduceOutput)v;
            int newUser = ((IntWritable)(value.getValue().get(new IntWritable(-1)))).get();
            int i = 0;
            ps.setInt(++i,iDimension.getDimensionByObject((key.getStatsCommonDismension().getDateDimension())));
            ps.setInt(++i,iDimension.getDimensionByObject(key.getStatsCommonDismension().getPlatformDimension()));
            ps.setInt(++i,iDimension.getDimensionByObject(key.getBrowserDimension()));
            ps.setInt(++i,newUser);
            ps.setString(++i,conf.get(GloadUtils.RUNNING_DATE));
            ps.setInt(++i,newUser);



            ps.addBatch();

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
