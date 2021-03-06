package com.phone.etl.analysis.dim.base;

import com.phone.etl.common.GloadUtils;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PlatformDimension extends BaseDimension {

    private int id;
    private String platformName;

    public PlatformDimension() {
    }

    public static PlatformDimension getInstance(String platformName){
        if(StringUtils.isEmpty(platformName)){
            return null;
        }
        return new PlatformDimension(platformName);
    }

    public PlatformDimension(String platformName) {
        this.platformName = platformName;
    }


    public static List<PlatformDimension> buildList(String platformName){
        if(StringUtils.isEmpty(platformName)){
            platformName = GloadUtils.DEFAULT_VALUE;
        }
        List<PlatformDimension> li = new ArrayList<PlatformDimension>();
        li.add(new PlatformDimension(platformName));
        li.add(new PlatformDimension(GloadUtils.ALL_OF_VALUE));
        return li;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(o==this){
            return 0;
        }
        PlatformDimension other = (PlatformDimension)o;

        int tmp = this.id -other.id;
        if(tmp!=0){
            return tmp;
        }

        return this.platformName.compareTo(((PlatformDimension) o).platformName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PlatformDimension that = (PlatformDimension) o;

        if (id != that.id) return false;
        return platformName != null ? platformName.equals(that.platformName) : that.platformName == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (platformName != null ? platformName.hashCode() : 0);
        return result;
    }

    public int getId() {

        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPlatformName() {
        return platformName;
    }

    public void setPlatformName(String platformName) {
        this.platformName = platformName;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(platformName);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id=dataInput.readInt();
        this.platformName=dataInput.readUTF();
    }
}
