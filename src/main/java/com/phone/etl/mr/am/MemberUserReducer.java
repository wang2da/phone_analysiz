package com.phone.etl.mr.am;

import com.phone.etl.analysis.dim.base.StatsUserDimension;
import com.phone.etl.output.map.MapOutput;
import com.phone.etl.output.reduce.ReduceOutput;
import com.phone.etl.utils.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MemberUserReducer extends Reducer<StatsUserDimension,MapOutput,StatsUserDimension,ReduceOutput> {

    private static final Logger logger = Logger.getLogger(MemberUserReducer.class);
    private ReduceOutput v = new ReduceOutput();
    private Set unique = new HashSet();
    private MapWritable map = new MapWritable();


    @Override
    protected void reduce(StatsUserDimension key, Iterable<MapOutput> values, Context context) throws IOException, InterruptedException {
        this.unique.clear();
        map.clear();
        for(MapOutput tv : values){
            this.unique.add(tv.getId());
        }

        if(key.getStatsCommonDismension().getKpiDimension().getKpiName().equals(KpiType.MEMBER_USER.kpiName)){
            this.v.setKpiType(KpiType.MEMBER_USER);
        }

        this.map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
        this.v.setValue(this.map);
        //修改 1
        this.v.setKpiType(KpiType.valueOfType(key.getStatsCommonDismension().getKpiDimension().getKpiName()));

        context.write(key,this.v);
    }
}
