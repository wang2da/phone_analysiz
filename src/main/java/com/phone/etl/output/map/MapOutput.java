package com.phone.etl.output.map;

import com.phone.etl.output.BaseOutput;
import com.phone.etl.utils.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapOutput extends BaseOutput {
    private String id;
    private long time;
    private KpiType kpi;

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public MapOutput() {
    }

    public MapOutput(String id, long time) {
        this.id = id;
        this.time = time;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MapOutput mapOutput = (MapOutput) o;

        if (time != mapOutput.time) return false;
        return id != null ? id.equals(mapOutput.id) : mapOutput.id == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (int) (time ^ (time >>> 32));
        return result;
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeLong(time);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readUTF();
        time = dataInput.readLong();
    }
}
