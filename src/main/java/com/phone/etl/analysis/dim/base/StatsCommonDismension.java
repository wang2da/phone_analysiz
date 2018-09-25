package com.phone.etl.analysis.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StatsCommonDismension extends StatsBaseDimension {

    public DateDimension dateDimension = new DateDimension();
    public KpiDimension kpiDimension = new KpiDimension();
    public PlatformDimension platformDimension = new PlatformDimension();

    public StatsCommonDismension() {
    }

    public StatsCommonDismension(DateDimension dateDimension, PlatformDimension platformDimension) {
        this.dateDimension = dateDimension;
        this.platformDimension = platformDimension;
    }

    public StatsCommonDismension(DateDimension dateDimension, KpiDimension kpiDimension, PlatformDimension platformDimension) {
        this.dateDimension = dateDimension;
        this.kpiDimension = kpiDimension;
        this.platformDimension = platformDimension;
    }



    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public KpiDimension getKpiDimension() {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {
        this.kpiDimension = kpiDimension;
    }

    public PlatformDimension getPlatformDimension() {
        return platformDimension;
    }

    public void setPlatformDimension(PlatformDimension platformDimension) {
        this.platformDimension = platformDimension;
    }

    @Override
    public int compareTo(BaseDimension o) {

        if(this == o){
            return 0;
        }

        StatsCommonDismension other = (StatsCommonDismension)o;
        int temp = this.dateDimension.compareTo(other.dateDimension);
        if(temp!=0){
            return temp;
        }

        temp = this.platformDimension.compareTo(other.platformDimension);
        if(temp!=0){
            return temp;
        }
        return this.kpiDimension.compareTo(kpiDimension);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dateDimension.write(dataOutput);
        platformDimension.write(dataOutput);
        kpiDimension.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.dateDimension.readFields(dataInput);
        platformDimension.readFields(dataInput);
        kpiDimension.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsCommonDismension that = (StatsCommonDismension) o;

        if (dateDimension != null ? !dateDimension.equals(that.dateDimension) : that.dateDimension != null)
            return false;
        if (kpiDimension != null ? !kpiDimension.equals(that.kpiDimension) : that.kpiDimension != null) return false;
        return platformDimension != null ? platformDimension.equals(that.platformDimension) : that.platformDimension == null;
    }

    @Override
    public int hashCode() {
        int result = dateDimension != null ? dateDimension.hashCode() : 0;
        result = 31 * result + (kpiDimension != null ? kpiDimension.hashCode() : 0);
        result = 31 * result + (platformDimension != null ? platformDimension.hashCode() : 0);
        return result;
    }
}
