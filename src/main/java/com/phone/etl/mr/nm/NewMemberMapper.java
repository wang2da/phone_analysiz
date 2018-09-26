package com.phone.etl.mr.nm;

import com.phone.etl.analysis.dim.base.*;
import com.phone.etl.output.map.MapOutput;
import com.phone.etl.utils.JdbcUtils;
import com.phone.etl.utils.KpiType;
import com.phone.etl.utils.MemberUtil;
import jodd.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;

public class NewMemberMapper extends Mapper<LongWritable,Text,StatsUserDimension,MapOutput>{

    private static final Logger logger = Logger.getLogger(NewMemberMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private MapOutput v = new MapOutput();
    private KpiDimension newUserKpi = new KpiDimension(KpiType.NEW_MEMBER.kpiName);
    private KpiDimension browserNewUserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_USER.kpiName);
    private Connection conn = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conn = JdbcUtils.getConn();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        JdbcUtils.close(conn,null,null);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(StringUtils.isEmpty(line)){
            return;
        }

        String[] fields = line.split("\001");
        String memberId = fields[4];
        String serverTime = fields[1];
        String platform = fields[13];

        if(StringUtils.isEmpty(memberId) || StringUtil.isEmpty(serverTime) || StringUtil.isEmpty(platform)){
            return;
        }

        if(!MemberUtil.checkMemberId(memberId)){
            return;
        }

        if(!MemberUtil.isNewMember(memberId,conn,context.getConfiguration())){
            return;
        }

        long serverTimeLong = Long.valueOf(serverTime);
        this.v.setId(memberId);
        this.v.setTime(serverTimeLong);

        PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
        DateDimension dateDimension = DateDimension.buildDate(serverTimeLong,DateEnum.DAY);
        BrowserDimension browserDimension = new BrowserDimension("","");


        StatsCommonDismension statsCommonDismension = this.k.getStatsCommonDismension();
        statsCommonDismension.setDateDimension(dateDimension);
        statsCommonDismension.setPlatformDimension(platformDimension);
        statsCommonDismension.setKpiDimension(newUserKpi);
        this.k.setBrowserDimension(browserDimension);
        this.k.setStatsCommonDismension(statsCommonDismension);


        context.write(this.k,this.v);

    }

}

