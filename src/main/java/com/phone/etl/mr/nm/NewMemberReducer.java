package com.phone.etl.mr.nm;

import com.phone.etl.analysis.dim.base.StatsUserDimension;
import com.phone.etl.output.map.MapOutput;
import com.phone.etl.output.reduce.ReduceOutput;
import com.phone.etl.utils.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class NewMemberReducer extends Reducer<StatsUserDimension,MapOutput,StatsUserDimension,ReduceOutput> {

    private static final Logger logger = Logger.getLogger(NewMemberReducer.class);
    private ReduceOutput v = new ReduceOutput();
    private Set<String> unique = new HashSet();
    private MapWritable map = new MapWritable();
    private Map<String,List<Long>> li = new HashMap<>();


    @Override
    protected void reduce(StatsUserDimension key, Iterable<MapOutput> values, Context context) throws IOException, InterruptedException {
        for(MapOutput tv : values){
            if(li.containsKey(tv.getId())){
                li.get(tv.getId()).add(tv.getTime());
            }else{
                List<Long> list = new ArrayList<>();
                list.add(tv.getTime());
                li.put(tv.getId(),list);
            }
        }

        for(Map.Entry<String,List<Long>> en : li.entrySet()){
            this.v.setKpiType(KpiType.MEMBER_INFO);
            this.map.put(new IntWritable(-2),new Text(en.getKey()));
            Collections.sort(en.getValue());
            this.map.put(new IntWritable(-3),new LongWritable(en.getValue().get(0)));
            context.write(key,this.v);

        }

        //修改 1
        this.v.setKpiType(KpiType.valueOfType(key.getStatsCommonDismension().getKpiDimension().getKpiName()));
        this.map.put(new IntWritable(-1),new IntWritable(this.li.size()));

        this.v.setValue(this.map);


        context.write(key,this.v);
        this.unique.clear();
    }
}
