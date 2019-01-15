package com.phone.etl.output.reduce;

import com.phone.etl.output.BaseOutput;
import com.phone.etl.utils.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextOutputValue extends BaseOutput{
    private String uuid = "";
    private String sessionId = "";

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.uuid);
        out.writeUTF(this.sessionId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uuid = in.readUTF();
        this.sessionId = in.readUTF();
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
