package com.phone.etl.mr;

import com.phone.etl.analysis.dim.base.StatsBaseDimension;
import com.phone.etl.analysis.dim.base.StatsUserDimension;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.mr.ua.service.impl.IDimensionImpl;
import com.phone.etl.output.reduce.ReduceOutput;
import com.phone.etl.utils.JdbcUtils;
import com.phone.etl.utils.KpiType;
import org.apache.calcite.prepare.Prepare;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

public class OutputToMysqlFormat extends OutputFormat<StatsUserDimension,ReduceOutput>{
    @Override
    public RecordWriter<StatsUserDimension, ReduceOutput> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        Connection conn = JdbcUtils.getConn();
        Configuration conf = context.getConfiguration();
        IDimension iDimension = new IDimensionImpl();

        return new OutputToMysqlRecordWritter(conf,conn,iDimension);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
                context);
    }

    public static class OutputToMysqlRecordWritter extends RecordWriter<StatsUserDimension,ReduceOutput>{
        Configuration conf = null;
        Connection conn = null;
        IDimension iDimension = null;

        private Map<KpiType,PreparedStatement> map = new HashMap<>();
        private Map<KpiType,Integer> batch = new HashMap<>();

        public OutputToMysqlRecordWritter(Configuration conf, Connection conn, IDimension iDimension) {
            this.conf = conf;
            this.conn = conn;
            this.iDimension = iDimension;
        }


        @Override
        public void write(StatsUserDimension key, ReduceOutput value) throws IOException, InterruptedException {
            KpiType kpi = value.getKpi();
            PreparedStatement ps = null;
            try{
                if(map.containsKey((kpi))){
                    ps = map.get(kpi);
                }else{
                    ps = conn.prepareStatement(conf.get(kpi.kpiName));
                    map.put(kpi,ps);
                }
                int count = 1;
                this.batch.put(kpi,count);
                count++;
                String className = conf.get("writter_"+kpi.kpiName);

//                String str ="writter_"+ conf.get(kpi.kpiName);

                Class<?> classz = Class.forName(className);
                IOWriterOutput writter = (IOWriterOutput) classz.newInstance();

                writter.output(conf,key,value,ps,iDimension);

                if(batch.size()%50 == 0){
                    ps.executeBatch();
//                    this.conn.commit();
                    batch.remove(kpi);
                }

            }catch (Exception e){
                e.printStackTrace();
            }


        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            try{
                for (Map.Entry<KpiType,PreparedStatement> en:map.entrySet()){
                    en.getValue().executeBatch();
//                    this.conn.commit();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            finally {
                for (Map.Entry<KpiType,PreparedStatement> en: map.entrySet()){
                    JdbcUtils.close(conn,en.getValue(),null);
                }
            }
        }
    }
}
