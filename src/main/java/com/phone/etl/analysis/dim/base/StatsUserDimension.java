package com.phone.etl.analysis.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StatsUserDimension extends BaseDimension{

    BrowserDimension browserDimension = new BrowserDimension();
    StatsCommonDismension  statsCommonDismension = new StatsCommonDismension();

    public StatsUserDimension() {
    }

    public BrowserDimension getBrowserDimension() {
        return browserDimension;
    }

    public void setBrowserDimension(BrowserDimension browserDimension) {
        this.browserDimension = browserDimension;
    }

    public StatsCommonDismension getStatsCommonDismension() {
        return statsCommonDismension;
    }

    public void setStatsCommonDismension(StatsCommonDismension statsCommonDismension) {
        this.statsCommonDismension = statsCommonDismension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsUserDimension that = (StatsUserDimension) o;

        if (browserDimension != null ? !browserDimension.equals(that.browserDimension) : that.browserDimension != null)
            return false;
        return statsCommonDismension != null ? statsCommonDismension.equals(that.statsCommonDismension) : that.statsCommonDismension == null;
    }

    @Override
    public int hashCode() {
        int result = browserDimension != null ? browserDimension.hashCode() : 0;
        result = 31 * result + (statsCommonDismension != null ? statsCommonDismension.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }

        StatsUserDimension other = (StatsUserDimension)o;
        int temp = this.statsCommonDismension.compareTo(other.statsCommonDismension);
        if(temp!=0){
            return temp;
        }

        return this.browserDimension.compareTo(browserDimension);

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        browserDimension.write(dataOutput);
        statsCommonDismension.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.browserDimension.readFields(dataInput);
        this.statsCommonDismension.readFields(dataInput);
    }
}
