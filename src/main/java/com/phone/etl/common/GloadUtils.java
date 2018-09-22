package com.phone.etl.common;

public class GloadUtils {
    public static String RUNNING_DATE="unknow";
    public static String DEFAULT_VALUE="unknow";
    public static String DEFAULT_ALL_VALUE="unknow";
    public static final String PREFIX_TOTAL = "total_";
    public static final long DAY_OF_MILISECONDS = 86400000L;//24*60*60*1000

    public static String DRIVER = "com.mysql.jdbc.Driver";
    public static String URL = "jdbc:mysql://hadoop01:3306/result";
    public static String USER = "root";
    public static String PASSWORD = "root";
    public static long DEFAULT_DATE_FORMAT = 86400000L;
}
