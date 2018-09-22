package com.phone.etl.mr.ua.service;

import com.phone.etl.analysis.dim.base.BaseDimension;

import java.io.IOException;
import java.sql.SQLException;

public interface IDimension {
    int getDimensionByObject(BaseDimension baseDimension)throws IOException,SQLException;
}
