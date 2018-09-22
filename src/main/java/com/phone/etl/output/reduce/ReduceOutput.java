package com.phone.etl.output.reduce;

import com.phone.etl.output.BaseOutput;
import com.phone.etl.utils.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReduceOutput extends BaseOutput {

    private KpiType kpiType;
    private MapWritable value = new MapWritable();

    public ReduceOutput() {
    }

    public ReduceOutput(KpiType kpiType, MapWritable value) {
        this.kpiType = kpiType;
        this.value = value;
    }

    public KpiType getKpiType() {
        return kpiType;
    }

    public void setKpiType(KpiType kpiType) {
        this.kpiType = kpiType;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    @Override
    public KpiType getKpi() {
        return this.kpiType;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeEnum(dataOutput,kpiType);
        this.value.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        WritableUtils.readEnum(dataInput,KpiType.class);
        this.value.readFields(dataInput);
    }

}
