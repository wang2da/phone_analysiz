package com.phone.etl.mr.etl;

import com.phone.etl.analysis.dim.base.BaseDimension;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.output.BaseOutput;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;

/**
 * @ClassName EtlToHdfs
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description
 * 原数据：/log/09/18
 * 原数据：/log/09/19
 * 清洗后的存储目录: /ods/09/18
 * 清洗后的存储目录: /ods/09/19
 **/
public class EtlToHdfs {


    public static interface IOWriterOutput {
        void output (Configuration conf, BaseDimension k,
                     BaseOutput v, PreparedStatement ps, IDimension iDimension) throws Exception;
    }
}