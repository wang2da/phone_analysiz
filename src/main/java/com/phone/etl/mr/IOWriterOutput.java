package com.phone.etl.mr;

import com.phone.etl.analysis.dim.base.BaseDimension;
import com.phone.etl.analysis.dim.base.StatsBaseDimension;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.output.BaseOutput;
import org.apache.hadoop.conf.Configuration;

import java.sql.PreparedStatement;

public interface IOWriterOutput {
    void output (Configuration conf, BaseDimension k,
                 BaseOutput v, PreparedStatement ps, IDimension iDimension) throws Exception;
}
