package com.phone.etl.mr.ua.service.impl;

import com.phone.etl.analysis.dim.base.*;
import com.phone.etl.mr.ua.service.IDimension;
import com.phone.etl.utils.JdbcUtils;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;

import java.io.IOException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

public class IDimensionImpl implements IDimension {
    private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return this.size() > 5000;
        }
    };


    @Override
    public int getDimensionByObject(BaseDimension baseDimension) throws IOException, SQLException {
        String cacheKey = this.buildCache(baseDimension);

        if (this.cache.containsKey(cacheKey)) {
            return this.cache.get(cacheKey);
        }

        String[] sqls = null;
        if (baseDimension instanceof DateDimension) {
            sqls = this.buildDateSqls();
        } else if (baseDimension instanceof PlatformDimension) {
            sqls = this.buildPlatFormSqls();
        } else if (baseDimension instanceof BrowserDimension) {
            sqls = this.buildBrowserSqls();
        } else if (baseDimension instanceof KpiDimension) {
            sqls = this.buildKpiSqls();
        }else if (baseDimension instanceof LocationDimension) {
            sqls = this.buildLocationSqls();
        }else if(baseDimension instanceof EventDimension){
            sqls = buildEventSqls(baseDimension);
        }else if(baseDimension instanceof CurrencyTypeDimension){
            sqls = buildCurrentSqls(baseDimension);
        }else if(baseDimension instanceof PaymentTypeDimension){
            sqls = this.buildPaymentSqls(baseDimension);
        }


        Connection conn = JdbcUtils.getConn();
        int id = -1;
        synchronized (this) {
            id = this.execute(sqls, baseDimension, conn);
        }

        this.cache.put(cacheKey, id);

        return id;
    }



    private int execute(String[] sqls, BaseDimension baseDimension, Connection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(sqls[0]);
            this.setArgs(baseDimension, ps);
            rs = ps.executeQuery();
            if(rs.next()){
                return rs.getInt(1);
            }
            ps = conn.prepareStatement(sqls[1],Statement.RETURN_GENERATED_KEYS);
            this.setArgs(baseDimension,ps);
            ps.executeUpdate();
            rs = ps.getGeneratedKeys();
            if(rs.next()){
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }


    private void setArgs(BaseDimension baseDimension, PreparedStatement ps) {
        try{
            int i = 0;
            if(baseDimension instanceof DateDimension){
                DateDimension date = (DateDimension)baseDimension;
                ps.setInt(++i,date.getYear());
                ps.setInt(++i,date.getSeason());
                ps.setInt(++i,date.getMonth());
                ps.setInt(++i,date.getWeek());
                ps.setInt(++i,date.getDay());
                ps.setDate(++i,new Date(date.getCalendar().getTime()));
                ps.setString(++i,date.getType());
            }else if(baseDimension instanceof PlatformDimension){
                PlatformDimension platformDimension = (PlatformDimension)baseDimension;
                ps.setString(++i,platformDimension.getPlatformName());
            }else if(baseDimension instanceof BrowserDimension){
                BrowserDimension browserDimension = (BrowserDimension)baseDimension;
                ps.setString(++i,browserDimension.getBrowserName());
                ps.setString(++i,browserDimension.getBrowserVersion());

            }else if(baseDimension instanceof KpiDimension){
                KpiDimension kpiDimension = (KpiDimension)baseDimension;
                ps.setString(++i,kpiDimension.getKpiName());
            }else if(baseDimension instanceof LocationDimension){
                LocationDimension locationDimension = (LocationDimension)baseDimension;
                ps.setString(++i,locationDimension.getCountry());
                ps.setString(++i,locationDimension.getProvince());
                ps.setString(++i,locationDimension.getCity());
            }else if(baseDimension instanceof EventDimension){
                EventDimension event = (EventDimension) baseDimension;
                ps.setString(++i,event.getCategory());
                ps.setString(++i,event.getAction());
            }else if(baseDimension instanceof CurrencyTypeDimension){
                CurrencyTypeDimension currency = (CurrencyTypeDimension) baseDimension;
                ps.setString(++i,currency.getCurrencyName());
            }else if(baseDimension instanceof PaymentTypeDimension){
                PaymentTypeDimension payment = (PaymentTypeDimension) baseDimension;
                ps.setString(++i,payment.getPaymentType());
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    private String[] buildEventSqls(BaseDimension dimension) {
        String query = "select id from `dimension_event` where `category` = ? and `action` = ? ";
        String insert = "insert into `dimension_event`(`category` , `action` ) values(?,?)";
        return new String[]{query,insert};
    }

    private String[] buildPaymentSqls(BaseDimension dimension) {
        String query = "select id from `dimension_payment_type` where `payment_type` = ?";
        String insert = "insert into `dimension_payment_type`(`payment_type`) values(?)";
        return new String[]{query,insert};
    }
    private String[] buildCurrentSqls(BaseDimension dimension) {
        String query = "select id from `dimension_currency_type` where `currency_name` = ?";
        String insert = "insert into `dimension_currency_type`(`currency_name`) values(?)";
        return new String[]{query,insert};
    }

    private String[] buildLocationSqls() {
        String select = "select `id` from `dimension_location` where `country` = ? and `province` = ? and `city` = ?";
        String insert = "insert into `dimension_location`(`country` ,`province`,`city`) values(?,?,?)";
        return new String[]{select,insert};
    }

    private String[] buildKpiSqls() {
        String select = "select id from dimension_kpi where kpi_name=?";
        String insert = "insert into dimension_kpi(kpi_name) values(?)";

        return new String[]{select,insert};
    }

    private String[] buildBrowserSqls() {
        String select = "select id from dimension_browser where browser_name=? and browser_version=?";
        String insert = "insert into dimension_browser (browser_name,browser_version) values(?,?)";

        return new String[] {select,insert};
    }

    private String[] buildPlatFormSqls() {
        String select = "select id from dimension_platform where platform_name=?";
        String insert = "insert into dimension_platform (platform_name) values(?)";

        return new String[]{select,insert};
    }

    private String[] buildDateSqls() {
        String select = "select id from dimension_date where year=? and season=? and month=? and week=? and day=? and calendar=? and type=?";
        String insert = "insert into dimension_date (year,season,month,week,day,calendar,type) values(?,?,?,?,?,?,?)";

        return new String[] {select,insert};
    }

    private String buildCache(BaseDimension baseDimension) {
        StringBuffer sb = new StringBuffer();
        if(baseDimension instanceof DateDimension){
            sb.append("date_");
            DateDimension date = (DateDimension)baseDimension;
            sb.append(date.getYear()).append(date.getSeason())
                    .append(date.getMonth())
                    .append(date.getWeek())
                    .append(date.getDay())
                    .append(date.getType());
        }else if(baseDimension instanceof PlatformDimension){
            sb.append("platform_");
            PlatformDimension date = (PlatformDimension)baseDimension;
            sb.append(date.getPlatformName());
        }else if(baseDimension instanceof BrowserDimension){
            sb.append("browser_");
            BrowserDimension date = (BrowserDimension)baseDimension;
            sb.append(date.getBrowserName())
            .append(date.getBrowserVersion());
        }else if(baseDimension instanceof KpiDimension){
            sb.append("platform_");
            KpiDimension date = (KpiDimension)baseDimension;
            sb.append(date.getKpiName());
        }else if(baseDimension instanceof LocationDimension){
            sb.append("platform_");
            LocationDimension date = (LocationDimension)baseDimension;
            sb.append(date.getCountry());
            sb.append(date.getProvince());
            sb.append(date.getCity());
        }else if(baseDimension instanceof EventDimension){
            EventDimension event = (EventDimension) baseDimension;
            sb.append("event_");
            sb.append(event.getCategory());
            sb.append(event.getAction());
        }else if(baseDimension instanceof CurrencyTypeDimension){
            sb.append("currency_");
            CurrencyTypeDimension currency = (CurrencyTypeDimension) baseDimension;
            sb.append(currency.getCurrencyName());
        }else if(baseDimension instanceof PaymentTypeDimension){
            sb.append("payment_");
            PaymentTypeDimension payment = (PaymentTypeDimension) baseDimension;
            sb.append(payment.getPaymentType());
        }
        return sb.length() == 0 ? null : sb.toString();
    }
}
