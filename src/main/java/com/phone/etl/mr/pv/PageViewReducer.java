package com.phone.etl.mr.pv;

import com.phone.etl.analysis.dim.base.StatsUserDimension;
import com.phone.etl.output.map.MapOutput;
import com.phone.etl.output.reduce.ReduceOutput;
import com.phone.etl.utils.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class PageViewReducer extends Reducer<StatsUserDimension,MapOutput,StatsUserDimension,ReduceOutput> {

    private static final Logger logger = Logger.getLogger(PageViewReducer.class);
    private ReduceOutput v = new ReduceOutput();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<MapOutput> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        for(MapOutput tv : values){
            count++;
        }

        MapWritable mapWritable = new MapWritable();

        mapWritable.put(new IntWritable(-1),new IntWritable(count));
        this.v.setValue(mapWritable);

        this.v.setKpiType(KpiType.valueOfType(key.getStatsCommonDismension().getKpiDimension().getKpiName()));

        context.write(key,this.v);
    }
}
