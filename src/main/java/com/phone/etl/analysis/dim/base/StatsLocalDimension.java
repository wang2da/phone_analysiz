package com.phone.etl.analysis.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StatsLocalDimension extends StatsBaseDimension {

    private StatsCommonDismension statsCommonDismension = new StatsCommonDismension();
    private LocationDimension locationDimension = new LocationDimension();

    public StatsLocalDimension() {
    }

    public StatsLocalDimension(StatsCommonDismension statsCommonDismension, LocationDimension locationDimension) {
        this.statsCommonDismension = statsCommonDismension;
        this.locationDimension = locationDimension;
    }

    public StatsCommonDismension getStatsCommonDismension() {

        return statsCommonDismension;
    }

    public void setStatsCommonDismension(StatsCommonDismension statsCommonDismension) {
        this.statsCommonDismension = statsCommonDismension;
    }

    public LocationDimension getLocationDimension() {
        return locationDimension;
    }

    public void setLocationDimension(LocationDimension locationDimension) {
        this.locationDimension = locationDimension;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(o==this){
            return 0;
        }
        StatsLocalDimension other = (StatsLocalDimension) o;
         int tmp = this.statsCommonDismension.compareTo(other.statsCommonDismension);
         if(tmp!=0){
             return tmp;
         }

         tmp = this.locationDimension.compareTo(other.locationDimension);

        return tmp;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsLocalDimension that = (StatsLocalDimension) o;

        if (statsCommonDismension != null ? !statsCommonDismension.equals(that.statsCommonDismension) : that.statsCommonDismension != null)
            return false;
        return locationDimension != null ? locationDimension.equals(that.locationDimension) : that.locationDimension == null;
    }

    @Override
    public int hashCode() {
        int result = statsCommonDismension != null ? statsCommonDismension.hashCode() : 0;
        result = 31 * result + (locationDimension != null ? locationDimension.hashCode() : 0);
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        statsCommonDismension.write(dataOutput);
        locationDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        statsCommonDismension.readFields(dataInput);
        locationDimension.readFields(dataInput);
    }
}
