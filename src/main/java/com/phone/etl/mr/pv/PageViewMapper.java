package com.phone.etl.mr.pv;

import com.phone.etl.analysis.dim.base.*;
import com.phone.etl.common.EventLogsConstant;
import com.phone.etl.output.map.MapOutput;
import com.phone.etl.utils.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class PageViewMapper extends Mapper<LongWritable,Text,StatsUserDimension,MapOutput>{

    private static final Logger logger = Logger.getLogger(PageViewMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private MapOutput v = new MapOutput();
    private KpiDimension pageviewKpi = new KpiDimension(KpiType.PAGEVIEW.kpiName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(StringUtils.isEmpty(line)){
            return;
        }

        String[] fields = line.split("\001");
        String en = fields[2];
        String serverTime = fields[1];
        String platform = fields[13];
        String browserName = fields[24];
        String browserVersion = fields[25];
        String url = fields[10];
        if(StringUtils.isNotEmpty(serverTime) && StringUtils.isNotEmpty(platform) && en.equals(EventLogsConstant.EventEnum.PAGEVIEW.alias)){

            Long stime = Long.valueOf(serverTime);
            this.v.setId(url);
            this.v.setTime(stime);

            DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
            PlatformDimension platformDimension = PlatformDimension.getInstance(platform);

            StatsCommonDismension statsCommonDismension = this.k.getStatsCommonDismension();
            statsCommonDismension.setDateDimension(dateDimension);
            statsCommonDismension.setPlatformDimension(platformDimension);


            BrowserDimension defaultBrowserDimension = BrowserDimension.newInstance(browserName,browserVersion);
            statsCommonDismension.setKpiDimension(pageviewKpi);
            this.k.setBrowserDimension(defaultBrowserDimension);
            this.k.setStatsCommonDismension(statsCommonDismension);

            context.write(this.k,this.v);
        }

    }
}
