package com.phone.etl.mr.na;

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

public class ActiveUserOutputWritter implements EtlToHdfs.IOWriterOutput {
    @Override
    public void output(Configuration conf, BaseDimension k, BaseOutput v, PreparedStatement ps, IDimension iDimension) throws Exception {
        try{
            StatsUserDimension key = (StatsUserDimension)k;
            ReduceOutput value = (ReduceOutput)v;
            int i = 0;
            switch (v.getKpi()) {
                case ACTIVE_USER:
                case BROWSER_ACTIVE_USER:
                    int newUser = ((IntWritable) (value.getValue().get(new IntWritable(-1)))).get();

                    ps.setInt(++i, iDimension.getDimensionByObject((key.getStatsCommonDismension().getDateDimension())));
                    ps.setInt(++i, iDimension.getDimensionByObject(key.getStatsCommonDismension().getPlatformDimension()));
                    ps.setInt(++i, newUser);
                    ps.setString(++i, conf.get(GloadUtils.RUNNING_DATE));
                    ps.setInt(++i, newUser);
                    break;
                case HOURLY_ACTIVE_USER:
                    ps.setInt(++i,iDimension.getDimensionByObject(key.getStatsCommonDismension().getDateDimension()));
                    ps.setInt(++i,iDimension.getDimensionByObject(key.getStatsCommonDismension().getPlatformDimension()));
                    ps.setInt(++i,iDimension.getDimensionByObject(key.getStatsCommonDismension().getKpiDimension()));

                    for (int j = 0;j<24;j++){
                        ps.setInt(++i,((IntWritable)(value.getValue().get(new IntWritable(j)))).get());
                    }
                    ps.setString(++i,conf.get(GloadUtils.RUNNING_DATE));
                    for (int j = 0;j<24;j++){
                        ps.setInt(++i,((IntWritable)(value.getValue().get(new IntWritable(j)))).get());
                    }
                    break;

            }

            ps.addBatch();

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
