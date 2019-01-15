package com.phone.etl.mr.local;

import com.phone.etl.analysis.dim.base.*;
import com.phone.etl.output.map.MapOutput;
import com.phone.etl.output.reduce.TextOutputValue;
import com.phone.etl.utils.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class LocalMapper extends Mapper<LongWritable,Text,StatsLocalDimension,TextOutputValue>{

    private static final Logger logger = Logger.getLogger(LocalMapper.class);
    private StatsLocalDimension k = new StatsLocalDimension();
    private TextOutputValue v = new TextOutputValue();
    private KpiDimension newUserKpi = new KpiDimension(KpiType.LOCAL.kpiName);
    private KpiDimension browserNewUserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_USER.kpiName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(StringUtils.isEmpty(line)){
            return;
        }

        String[] fields = line.split("\001");
        String uuid = fields[3];
        String sessionId = fields[5];
        String serverTime = fields[1];
        Long stime = Long.valueOf(serverTime);
        String platform = fields[13];
        String country = fields[28];
        String province = fields[29];
        String city = fields[30];

        if(StringUtils.isEmpty(serverTime)){
            logger.info("serverTime | uuid is null.");
            return;
        }

        this.v.setUuid(uuid);
        this.v.setSessionId(sessionId);


        DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
        PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
        LocationDimension locationDimension = LocationDimension.getInstance(country,province,city);


        StatsCommonDismension statsCommonDismension = this.k.getStatsCommonDismension();
        statsCommonDismension.setDateDimension(dateDimension);

        BrowserDimension defaultBrowserDimension = new BrowserDimension("","");
        statsCommonDismension.setPlatformDimension(platformDimension);
        statsCommonDismension.setKpiDimension(newUserKpi);
//        statsCommonDismension.setLocationDimension(locationDimension);
//        this.k.setBrowserDimension(defaultBrowserDimension);
        this.k.setLocationDimension(locationDimension);
        this.k.setStatsCommonDismension(statsCommonDismension);



        context.write(this.k,this.v);

    }
}
