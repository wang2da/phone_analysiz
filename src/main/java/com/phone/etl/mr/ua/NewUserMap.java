package com.phone.etl.mr.ua;

import com.phone.etl.analysis.dim.base.StatsUserDimension;
import com.phone.etl.output.map.MapOutput;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NewUserMap extends Mapper<LongWritable,Text,StatsUserDimension,MapOutput> {
}
