package com.phone.etl.mr.na;

import com.phone.etl.analysis.dim.base.DateEnum;
import com.phone.etl.analysis.dim.base.StatsUserDimension;
import com.phone.etl.output.map.MapOutput;
import com.phone.etl.output.reduce.ReduceOutput;
import com.phone.etl.utils.KpiType;
import com.phone.etl.utils.TimeUtil;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ActiveUserReducer extends Reducer<StatsUserDimension,MapOutput,StatsUserDimension,ReduceOutput> {

    private static final Logger logger = Logger.getLogger(ActiveUserReducer.class);
    private ReduceOutput v = new ReduceOutput();
    private Set unique = new HashSet();
    private MapWritable map = new MapWritable();
    private Map<Integer,Set<String>> hourlyMap = new HashedMap();
    private MapWritable houlyWritable = new MapWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        for (int i=0;i<=23;i++){
            this.hourlyMap.put(i,new HashSet<>());
            this.houlyWritable.put(new IntWritable(i),new IntWritable(0));

        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<MapOutput> values, Context context) throws IOException, InterruptedException {
        try {
            this.unique.clear();
            map.clear();
            for(MapOutput tv : values){
                this.unique.add(tv.getId());
                if(key.getStatsCommonDismension().getKpiDimension().getKpiName().equals(KpiType.ACTIVE_USER.kpiName)){
                    int hour = TimeUtil.getDateInfo(tv.getTime(), DateEnum.HOUR);
                    this.hourlyMap.get(hour).add(tv.getId());
                }
            }

            if(key.getStatsCommonDismension().getKpiDimension().getKpiName().equals(KpiType.ACTIVE_USER.kpiName)){
                for (Map.Entry<Integer,Set<String>> en : hourlyMap.entrySet()){
                    this.houlyWritable.put(new IntWritable(en.getKey()),new IntWritable(en.getValue().size()));
                }
                this.v.setKpiType(KpiType.HOURLY_ACTIVE_USER);
                this.v.setValue(this.houlyWritable);
                context.write(key,this.v);
            }

            this.map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
            this.v.setValue(this.map);
            //修改 1
            this.v.setKpiType(KpiType.valueOfType(key.getStatsCommonDismension().getKpiDimension().getKpiName()));

            context.write(key,this.v);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            this.unique.clear();
            this.hourlyMap.clear();
            this.houlyWritable.clear();
            for(int i = 0; i < 24 ; i++){
                this.hourlyMap.put(i,new HashSet<String>());
                this.houlyWritable.put(new IntWritable(i),new IntWritable(0));
            }
        }
    }
}
