package com.phone.etl.output;

import com.phone.etl.utils.KpiType;
import org.apache.hadoop.io.Writable;

public abstract class BaseOutput implements Writable {
    public abstract KpiType getKpi();
}
