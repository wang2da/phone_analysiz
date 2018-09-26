package com.phone.etl.mr.am;

import com.phone.etl.analysis.dim.base.*;
import com.phone.etl.output.map.MapOutput;
import com.phone.etl.utils.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class MemberUserMapper extends Mapper<LongWritable,Text,StatsUserDimension,MapOutput>{

    private static final Logger logger = Logger.getLogger(MemberUserMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private MapOutput v = new MapOutput();
    private KpiDimension newUserKpi = new KpiDimension(KpiType.MEMBER_USER.kpiName);
    private KpiDimension browserNewUserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_USER.kpiName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(StringUtils.isEmpty(line)){
            return;
        }

        String[] fields = line.split("\001");
        String en = fields[2];
        String u_mid = fields[4];
        if(StringUtils.isNotEmpty(en) && StringUtils.isNotEmpty(u_mid)){
            String serverTime = fields[1];
            String platform = fields[13];
            String uuid = fields[3];
            String browserName = fields[24];
            String browserVersion = fields[25];
            if(StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(u_mid)){
                logger.info("serverTime | uuid is null.");
                return;
            }

            Long stime = Long.valueOf(serverTime);
            DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
            PlatformDimension platformDimension = PlatformDimension.getInstance(platform);

            StatsCommonDismension statsCommonDismension = this.k.getStatsCommonDismension();
            statsCommonDismension.setDateDimension(dateDimension);

            BrowserDimension defaultBrowserDimension = new BrowserDimension("","");
            statsCommonDismension.setPlatformDimension(platformDimension);
            statsCommonDismension.setKpiDimension(newUserKpi);
            this.k.setBrowserDimension(defaultBrowserDimension);
            this.k.setStatsCommonDismension(statsCommonDismension);

            this.v.setId(u_mid);
            this.v.setTime(stime);
//

            context.write(this.k,this.v);

        }

    }
}
