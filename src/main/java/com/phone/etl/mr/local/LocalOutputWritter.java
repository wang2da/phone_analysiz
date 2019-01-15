package com.phone.etl.mr.local;

import com.phone.etl.analysis.dim.base.BaseDimension;
import com.phone.etl.analysis.dim.base.StatsLocalDimension;
import com.phone.etl.common.GloadUtils;
import com.phone.etl.mr.etl.EtlToHdfs;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.output.BaseOutput;
import com.phone.etl.output.reduce.LocalOutputValue;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;

public class LocalOutputWritter implements EtlToHdfs.IOWriterOutput {
    @Override
    public void output(Configuration conf, BaseDimension k, BaseOutput v, PreparedStatement ps, IDimension iDimension) throws Exception {
        try{


            StatsLocalDimension key = (StatsLocalDimension) k;
            LocalOutputValue value = (LocalOutputValue) v;
            int i = 0;
            ps.setInt(++i,iDimension.getDimensionByObject(key.getStatsCommonDismension().getDateDimension()));
            ps.setInt(++i,iDimension.getDimensionByObject(key.getStatsCommonDismension().getPlatformDimension()));
            ps.setInt(++i,iDimension.getDimensionByObject(key.getLocationDimension()));
            ps.setInt(++i,value.getAus());
            ps.setInt(++i,value.getSessions());
            ps.setInt(++i,value.getBoundSessions());
            ps.setString(++i,conf.get(GloadUtils.RUNNING_DATE));
            ps.setInt(++i,value.getAus());
            ps.setInt(++i,value.getSessions());
            ps.setInt(++i,value.getBoundSessions());

            ps.addBatch();

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
