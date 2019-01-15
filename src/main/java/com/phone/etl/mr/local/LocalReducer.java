package com.phone.etl.mr.local;

import com.phone.etl.analysis.dim.base.StatsLocalDimension;
import com.phone.etl.analysis.dim.base.StatsUserDimension;
import com.phone.etl.output.reduce.LocalOutputValue;
import com.phone.etl.output.reduce.ReduceOutput;
import com.phone.etl.output.reduce.TextOutputValue;
import com.phone.etl.utils.KpiType;
import jodd.util.StringUtil;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LocalReducer extends Reducer<StatsLocalDimension,TextOutputValue,StatsLocalDimension,LocalOutputValue> {

    private static final Logger logger = Logger.getLogger(LocalReducer.class);
//    private ReduceOutput v = new ReduceOutput();
    private Set unique = new HashSet();
    private Map<String,Integer> map2 = new HashMap<>();
    private LocalOutputValue v = new LocalOutputValue();
    private MapWritable map = new MapWritable();


    @Override
    protected void reduce(StatsLocalDimension key, Iterable<TextOutputValue> values, Context context) throws IOException, InterruptedException {
        this.unique.clear();
        this.map.clear();
        for(TextOutputValue tv : values){
            if(StringUtil.isNotEmpty(tv.getUuid())){
                this.unique.add(tv.getUuid());
            }
            if(StringUtil.isNotEmpty(tv.getSessionId())){
                if(map2.containsKey(tv.getSessionId())){
                    map2.put(tv.getSessionId(),2);
                }else{
                    map2.put(tv.getSessionId(),1);
                }
            }
        }


        this.v.setAus(this.unique.size());
        this.v.setSessions(this.map2.size());

        int boundSessions=0;
        for (Map.Entry<String,Integer> en : map2.entrySet()){
            if(en.getValue()<2){
                boundSessions++;
            }
        }

        this.v.setBoundSessions(boundSessions);
//        this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDismension().getKpiDimension().getKpiName()));
        this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDismension().getKpiDimension().getKpiName()));
        context.write(key,this.v);
    }
}
