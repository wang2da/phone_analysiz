package com.phone.etl.mr.session;

import com.phone.etl.analysis.dim.base.*;
import com.phone.etl.common.EventLogsConstant;
import com.phone.etl.output.map.MapOutput;
import com.phone.etl.utils.KpiType;
import jodd.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class SessionMapper extends Mapper<LongWritable,Text,StatsUserDimension,MapOutput>{

    private static final Logger logger = Logger.getLogger(SessionMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private MapOutput v = new MapOutput();
    private KpiDimension newUserKpi = new KpiDimension(KpiType.SESSIONS.kpiName);
    private KpiDimension browserNewUserKpi = new KpiDimension(KpiType.BROWSER_NEW_USER.kpiName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(StringUtils.isEmpty(line)){
            return;
        }

        String[] fields = line.split("\001");
        String en = fields[2];
        String u_sd = fields[5];
        String serverTime = fields[1];
        String platform = fields[13];
        if(StringUtils.isNotEmpty(en) && StringUtil.isNotEmpty(u_sd)){

            Long stime = Long.valueOf(serverTime);
            DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
            PlatformDimension platformDimension = PlatformDimension.getInstance(platform);

            StatsCommonDismension statsCommonDismension = this.k.getStatsCommonDismension();
            statsCommonDismension.setDateDimension(dateDimension);
            statsCommonDismension.setPlatformDimension(platformDimension);


            BrowserDimension defaultBrowserDimension = new BrowserDimension("","");
            statsCommonDismension.setKpiDimension(newUserKpi);
            this.k.setBrowserDimension(defaultBrowserDimension);
            this.k.setStatsCommonDismension(statsCommonDismension);
            this.v.setId(u_sd);
            this.v.setTime(stime);


            context.write(this.k,this.v);
        }

    }
}
