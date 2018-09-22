package com.phone.etl.mr.ua;

import com.phone.etl.analysis.dim.base.StatsUserDimension;
import com.phone.etl.output.map.MapOutput;
import com.phone.etl.output.reduce.ReduceOutput;
import org.apache.hadoop.mapreduce.Reducer;

public class NewUserReduce extends Reducer<StatsUserDimension,MapOutput,StatsUserDimension,ReduceOutput> {

}
