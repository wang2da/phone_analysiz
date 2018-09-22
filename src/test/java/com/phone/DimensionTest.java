package com.phone;

import com.phone.etl.analysis.dim.base.DateDimension;
import com.phone.etl.analysis.dim.base.DateEnum;
import com.phone.etl.analysis.dim.base.PlatformDimension;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.mr.ua.service.impl.IDimensionImpl;

import java.io.IOException;
import java.sql.SQLException;

public class DimensionTest {
    public static void main(String[] args) {
        PlatformDimension pl = new PlatformDimension("ios");
        DateDimension dt = DateDimension.buildDate(324789342343829L, DateEnum.DAY);
        IDimension iDimension = new IDimensionImpl();
        try {
            System.out.println(iDimension.getDimensionByObject(pl));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
