package com.phone.etl.analysis.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KpiDimension extends BaseDimension{

    private int id;
    private String kpiName;

    public KpiDimension() {
    }
    public KpiDimension(String kpiName) {
        this.id = id;
        this.kpiName = kpiName;
    }

    public KpiDimension(int id, String kpiName) {
        this.id = id;
        this.kpiName = kpiName;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getKpiName() {
        return kpiName;
    }

    public void setKpiName(String kpiName) {
        this.kpiName = kpiName;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(o==this){
            return 0;
        }
        KpiDimension other = (KpiDimension)o;

        int tmp = this.id -other.id;
        if(tmp!=0){
            return tmp;
        }

        tmp = this.kpiName.compareTo(other.kpiName);
        if(tmp!=0){
            return tmp;
        }

        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(kpiName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.kpiName= dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KpiDimension that = (KpiDimension) o;

        if (id != that.id) return false;
        return kpiName != null ? kpiName.equals(that.kpiName) : that.kpiName == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (kpiName != null ? kpiName.hashCode() : 0);
        return result;
    }
}
