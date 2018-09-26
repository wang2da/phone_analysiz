package com.phone.etl.mr.nm;

import com.phone.etl.analysis.dim.base.DateDimension;
import com.phone.etl.analysis.dim.base.DateEnum;
import com.phone.etl.analysis.dim.base.StatsUserDimension;
import com.phone.etl.common.GloadUtils;
import com.phone.etl.mr.OutputToMysqlFormat;
import com.phone.etl.mr.am.MemberUserMapper;
import com.phone.etl.mr.am.MemberUserReducer;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.mr.ua.service.impl.IDimensionImpl;
import com.phone.etl.output.map.MapOutput;
import com.phone.etl.output.reduce.ReduceOutput;
import com.phone.etl.utils.JdbcUtils;
import com.phone.etl.utils.TimeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class NewMemberRunner implements Tool {
    private static final Logger logger = Logger.getLogger(NewMemberRunner.class);
    private Configuration conf = new Configuration();

    //主函数---入口
    public static void main(String[] args){
        try {
            ToolRunner.run(new Configuration(),new NewMemberRunner(),args);
        } catch (Exception e) {
            logger.warn("NEW_USER TO MYSQL is failed !!!",e);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = this.getConf();
        conf.set("fs.defaultFS","hdfs://hadoop01:8020");
        System.setProperty("HADOOP_USER_NAME","root");
        this.setArgs(strings,conf);


        Job job = Job.getInstance(conf,"NEW_USER TO MYSQL");

        job.setJarByClass(NewMemberRunner.class);

        //设置map相关参数
        job.setMapperClass(NewMemberMapper.class);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(MapOutput.class);

        //设置reduce相关参数
        job.setReducerClass(NewMemberReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(ReduceOutput.class);
        job.setOutputFormatClass(OutputToMysqlFormat.class);

        //设置reduce task的数量
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job,new Path("/odl/09/18/part-m-00000"));

        //设置输入参数
//        FileInputFormat.setInputPaths(job,new Path("/ods/09/18/part-m-00000"));
        return job.waitForCompletion(true)? 0:1;
//        if(job.waitForCompletion(true)){
//            this.computeNewTotalUser(job);
//            return 0;
//        }else {
//            return 1;
//        }
    }

    @Override
    public void setConf(Configuration configuration) {
        configuration.addResource("output_mapping.xml");
        configuration.addResource("output_writter.xml");
        this.conf = configuration;

    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
//    private static Configuration conf = new Configuration();
//    private static Logger logger = Logger.getLogger(NewUserRunner.class);
//
//    public static void main(String[] args) {
//        try {
//            ToolRunner.run(new Configuration(),new NewUserRunner(),args);
//        } catch (Exception e) {
//            logger.error("运行新增用户指标失败.",e);
//        }
//    }
//
//
//    @Override
//    public int run(String[] strings) throws Exception {
//        Configuration conf = getConf();
//        conf.set("fs.defaultFS","hdfs://hadoop01:8020");
//        System.setProperty("HADOOP_USER_NAME","root");
//        this.setArgs(strings,conf);
//
//        Job job = Job.getInstance(conf,"NewUserRunner");
//        job.setJarByClass(NewUserRunner.class);
//        job.setMapperClass(NewUserMapper.class);
//        job.setMapOutputKeyClass(StatsUserDimension.class);
//        job.setMapOutputValueClass(MapOutput.class);
//
//
//        job.setReducerClass(NewUserReducer.class);
//        job.setOutputKeyClass(StatsUserDimension.class);
//        job.setOutputValueClass(ReduceOutput.class);
//
//        FileInputFormat.setInputPaths(job,new Path("/odl/09/18"));
//        job.setOutputFormatClass(OutputToMysqlFormat.class);
////        handleInputAndOutput(job);
//
//        return job.waitForCompletion(true)?0:1;
//
////        if(job.waitForCompletion(true)){
////            this.computeNewTotalUser(job);
////            return 0;
////        }else {
////            return 1;
////        }
//    }
    private void handleInputAndOutput(Job job) {
        String[] fields = job.getConfiguration().get(GloadUtils.RUNNING_DATE).split("-");
        String month = fields[1];
        String day = fields[2];
        try{
            Path inputPath = new Path("/log/2018/" + month + "/" + day);
//            Path outputPath = new Path("/odl/" + month + "/" + day);
            FileSystem fs = FileSystem.get(job.getConfiguration());
            if(fs.exists(inputPath)){
                FileInputFormat.addInputPath(job,inputPath);
            }

//            if(fs.exists(outputPath)){
//                fs.delete(outputPath);
//            }
//            FileOutputFormat.setOutputPath(job,outputPath);


        }catch (Exception e){
            logger.error("设置路径错误",e);
        }

    }


//    @Override
//    public void setConf(Configuration configuration) {
//        configuration.addResource("output_mapping.xml");
//        configuration.addResource("writter_mapping.xml");
//        this.conf = configuration;
//    }
//
//    @Override
//    public Configuration getConf() {
//        return conf;
//    }

    private void computeNewTotalUser(Job job) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //获取运行的日期
            String date = job.getConfiguration().get(GloadUtils.RUNNING_DATE);//2018-09-18
            long nowDay = TimeUtil.parserString2Long(date);
            long yesterDay = nowDay - GloadUtils.DAY_OF_MILISECONDS;

            //获取对应的时间维度
            DateDimension nowDateDimension = DateDimension.buildDate(nowDay, DateEnum.DAY);
            DateDimension yesterdayDateDimension = DateDimension.buildDate(yesterDay, DateEnum.DAY);

            int nowDimensionId = -1;
            int yesterDimensionId = -1;

            //获取维度的id
            IDimension convert = new IDimensionImpl();
            nowDimensionId = convert.getDimensionByObject(nowDateDimension);
            yesterDimensionId = convert.getDimensionByObject(yesterdayDateDimension);

            //判断昨天和今天的维度Id是否大于0
            conn = JdbcUtils.getConn();
            Map<String,Integer> map = new HashMap<String,Integer>();
            if(yesterDimensionId > 0){
                ps = conn.prepareStatement(conf.get(GloadUtils.PREFIX_TOTAL+"new_total_user"));
                //赋值
                ps.setInt(1,yesterDimensionId);
                //执行
                rs = ps.executeQuery();
                while (rs.next()){
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalNewUser = rs.getInt("total_install_users");
                    //存储
                    map.put(platformId+"",totalNewUser);
                }
            }

            if(nowDimensionId > 0){
                ps = conn.prepareStatement(conf.get(GloadUtils.PREFIX_TOTAL+"user_new_user"));
                //赋值
                ps.setInt(1,nowDimensionId);
                //执行
                rs = ps.executeQuery();
                while (rs.next()){
                    int platformId = rs.getInt("platform_dimension_id");
                    int newUser = rs.getInt("new_install_users");
                    //存储
                    if(map.containsKey(platformId+"")){
                        newUser += map.get(platformId+"");
                    }
                    map.put(platformId+"",newUser);
                }
            }

            //更新新增的总用户
            ps = conn.prepareStatement(conf.get(GloadUtils.PREFIX_TOTAL+"user_new_update_user"));
            //赋值
            for (Map.Entry<String,Integer> en:map.entrySet()){
                ps.setInt(1,nowDimensionId);
                ps.setInt(2,Integer.parseInt(en.getKey()));
                ps.setInt(3,en.getValue());
                ps.setString(4,conf.get(GloadUtils.RUNNING_DATE));
                ps.setInt(5,en.getValue());
                ps.execute();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtils.close(conn,ps,rs);
        }
    }

    private void setArgs(String[] args, Configuration conf) {
        String date = null;
        for (int i = 0;i < args.length;i++){
            if(args[i].equals("-d")){
                if(i+1 < args.length){
                    date = args[i+1];
                    break;
                }
            }
        }
        //代码到这儿，date还是null，默认用昨天的时间
        if(date == null){
            date = TimeUtil.getYesterdayDate();
        }
        //然后将date设置到时间conf中
        conf.set(GloadUtils.RUNNING_DATE,date);
    }


}
