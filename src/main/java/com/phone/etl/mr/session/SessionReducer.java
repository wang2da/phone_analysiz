package com.phone.etl.mr.session;

import com.phone.etl.analysis.dim.base.StatsUserDimension;
import com.phone.etl.common.GloadUtils;
import com.phone.etl.output.map.MapOutput;
import com.phone.etl.output.reduce.ReduceOutput;
import com.phone.etl.utils.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class SessionReducer extends Reducer<StatsUserDimension,MapOutput,StatsUserDimension,ReduceOutput> {

    private static final Logger logger = Logger.getLogger(SessionReducer.class);
    private ReduceOutput v = new ReduceOutput();
    private Set unique = new HashSet();
    private Map<String,List<Long>> map = new HashMap<String,List<Long>>();


    @Override
    protected void reduce(StatsUserDimension key, Iterable<MapOutput> values, Context context) throws IOException, InterruptedException {

        this.unique.clear();
//        this.map.clear();

        for(MapOutput tv : values){
            String sessionId = tv.getId();
            long serverTime = tv.getTime();
            if(map.containsKey(tv.getId())){
                List<Long> li = map.get(sessionId);
                li.add(serverTime);
                map.put(sessionId,li);
            }else{
                List<Long> li = new ArrayList<>();
                li.add(serverTime);
                map.put(sessionId,li);
            }
        }

        System.out.println("map:"+map.toString());
        MapWritable mapWritable = new MapWritable();
        mapWritable.put(new IntWritable(-1),new IntWritable(this.map.size()));

        int sessionLength = 0;
        for (Map.Entry<String,List<Long>> en:map.entrySet()) {
            if(en.getValue().size() >= 2){
                Collections.sort(en.getValue());
                sessionLength += (en.getValue().get(en.getValue().size()-1) - en.getValue().get(0));
                System.out.println(en.getKey()+"aaaaaa:"+sessionLength);
            }
        }

        if(sessionLength>0 && sessionLength<= GloadUtils.DAY_OF_MILISECONDS){
            if(sessionLength%1000==0){
                sessionLength=sessionLength/1000;
            }else{
                sessionLength = sessionLength/1000 + 1;
            }

        }


        if(key.getStatsCommonDismension().getKpiDimension().getKpiName().equals(KpiType.SESSIONS.kpiName)){
            this.v.setKpiType(KpiType.SESSIONS);
        }

        mapWritable.put(new IntWritable(-2),new IntWritable(sessionLength));
        this.v.setValue(mapWritable);
        //修改 1
        this.v.setKpiType(KpiType.valueOfType(key.getStatsCommonDismension().getKpiDimension().getKpiName()));

        context.write(key,this.v);
    }
}
