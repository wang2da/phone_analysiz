package com.phone.etl.mr.etl;


import com.phone.etl.common.GloadUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class EtlToHdfsRunner implements Tool {
    static Logger logger = Logger.getLogger(EtlToHdfsRunner.class);
    Configuration conf = null;

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(),new EtlToHdfsRunner(),args);
        } catch (Exception e) {
            logger.warn("执行etl to hdfs异常.",e);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("fs.defaultFS","hdfs://hadoop01:8020");
        System.setProperty("HADOOP_USER_NAME","root");
        this.handleArgs(conf,strings);

        Job job = Job.getInstance(conf,"EtlToHdfsRunner");
        job.setJarByClass(EtlToHdfsRunner.class);
        job.setMapperClass(EtlToHdfsMapper.class);
        job.setMapOutputKeyClass(LogWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        handleInputAndOutput(job);


        return job.waitForCompletion(true)?1:0;
    }

    private void handleInputAndOutput(Job job) {
        String[] fields = job.getConfiguration().get(GloadUtils.RUNNING_DATE).split("-");
        String month = fields[1];
        String day = fields[2];
        try{
            Path inputPath = new Path("/log/" + month + "/" + day);
            Path outputPath = new Path("/odl/" + month + "/" + day);
            FileSystem fs = FileSystem.get(job.getConfiguration());
            if(fs.exists(inputPath)){
                FileInputFormat.addInputPath(job,inputPath);
            }

            if(fs.exists(outputPath)){
                fs.delete(outputPath);
            }
            FileOutputFormat.setOutputPath(job,outputPath);


        }catch (Exception e){
            logger.error("设置路径错误",e);
        }



    }

    private void handleArgs(Configuration conf, String[] strings) {
        String date = null;
        if(strings.length>0){
            for (int i=0;i<strings.length;i++){
                if("-d".equals(strings[i])){
                    if(i+1 <= strings.length+1){
                        date=strings[i+1];
                        break;
                    }
                }
            }
            if(StringUtils.isEmpty(date)){

            }
        }
        conf.set(GloadUtils.RUNNING_DATE,date);
    }

    @Override
    public void setConf(Configuration configuration) {
        conf = new Configuration();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
