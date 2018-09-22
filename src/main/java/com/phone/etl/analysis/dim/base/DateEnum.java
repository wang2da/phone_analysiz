package com.phone.etl.analysis.dim.base;

public enum DateEnum {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    public String dateType;

    public String getdateType() {
        return dateType;
    }

    public void setdateType(String dateType) {
        this.dateType = dateType;
    }


    DateEnum(String dateType){
        this.dateType=dateType;
    }
    public static DateEnum valueofdateType(String dateType){
        for(DateEnum date : values()){
            if(dateType.equals(date.dateType)){
                return date;
            }
        }
        throw new RuntimeException("运行时异常");
    }
}
