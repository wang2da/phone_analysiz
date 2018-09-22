package com.phone.etl.analysis.dim.base;


import com.phone.etl.utils.TimeUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

public class DateDimension extends BaseDimension {

    private int id;
    private int year;
    private int season;
    private int month;
    private int week;
    private int day;
    private Date calendar = new Date();
    private String type;

    public DateDimension() {
    }

     public DateDimension(int year, int season, int month, int week, int day, String type, Date calendar) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
        this.type = type;
        this.calendar = calendar;
    }

    public DateDimension(int id, int year, int season, int month, int week, int day, String type, Date calendar) {
        this(year,season,month,week,day,type,calendar);
        this.id = id;
    }

//    public static List<DateDimension> buildList(Long time,DateEnum type){
//       int year = TimeUnit.getDateInfo(time,type);

//        Calendar calendar = Calendar.getInstance();
//        calendar.clear();
//        if(DateEnum.YEAR.equals("type")){
//            calendar.set(year,0,1);
//            return new DateDimension(year,)
//    }

//        return null;
//    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DateDimension that = (DateDimension) o;

        if (id != that.id) return false;
        if (year != that.year) return false;
        if (season != that.season) return false;
        if (month != that.month) return false;
        if (week != that.week) return false;
        if (day != that.day) return false;
        if (calendar != null ? !calendar.equals(that.calendar) : that.calendar != null) return false;
        return type != null ? type.equals(that.type) : that.type == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + year;
        result = 31 * result + season;
        result = 31 * result + month;
        result = 31 * result + week;
        result = 31 * result + day;
        result = 31 * result + (calendar != null ? calendar.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }

        DateDimension other = (DateDimension)o;
        int temp = this.id-other.id;
        if(temp!=0){
            return temp;
        }

        temp = this.year-other.year;
        if(temp!=0){
            return temp;
        }
        temp = this.season-other.season;
        if(temp!=0){
            return temp;
        }
        temp = this.month-other.month;
        if(temp!=0){
            return temp;
        }
        temp = this.week-other.week;
        if(temp!=0){
            return temp;
        }
        temp = this.day-other.day;
        if(temp!=0){
            return temp;
        }

        return this.type.compareTo(((DateDimension) o).type);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeInt(year);
        dataOutput.writeInt(season);
        dataOutput.writeInt(month);
        dataOutput.writeInt(week);
        dataOutput.writeInt(day);
        dataOutput.writeUTF(type);
        dataOutput.writeLong(this.calendar.getTime()); //date类型
    }

    public Date getCalendar() {
        return calendar;
    }

    public void setCalendar(Date calendar) {
        this.calendar = calendar;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id= dataInput.readInt();
        this.year= dataInput.readInt();
        this.season= dataInput.readInt();
        this.month= dataInput.readInt();
        this.week= dataInput.readInt();
        this.day= dataInput.readInt();
        this.type = dataInput.readUTF();
        this.calendar.setTime(dataInput.readLong()); //日期变长long类型
    }

    //time 时间戳 毫秒
    //type 指标类型
    public static DateDimension buildDate(long time,DateEnum type){

        int year = TimeUtil.getDateInfo(time,DateEnum.YEAR);
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        if(type.equals(DateEnum.YEAR)){
            calendar.setTimeInMillis(time);
            return new DateDimension(year,0,0,0,1,type.dateType,calendar.getTime());
        }

        int season = TimeUtil.getDateInfo(time,DateEnum.SEASON);
        if(type.equals((DateEnum.SEASON))){
            int month = season*3-2;
            calendar.set(year,month-1,1);
            return new DateDimension(year,season,month,0,1,type.dateType,calendar.getTime());
        }

        int month = TimeUtil.getDateInfo(time,DateEnum.MONTH);
        if(type.equals(DateEnum.MONTH)){
            calendar.set(year,month-1,1);
            return new DateDimension(year,season,month,0,1,type.dateType,calendar.getTime());
        }

        int week = TimeUtil.getDateInfo(time,DateEnum.WEEK);
        if(type.equals(DateEnum.WEEK)){
            long firstDayOfWeek = TimeUtil.getFirstDayOfWeek(time);
            TimeUtil.getDateInfo(firstDayOfWeek,DateEnum.YEAR);
            TimeUtil.getDateInfo(firstDayOfWeek,DateEnum.SEASON);
            TimeUtil.getDateInfo(firstDayOfWeek,DateEnum.MONTH);

            int day = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.DAY);

            calendar.set(year,month-1,day);
            return new DateDimension(year,season,month,week,day,type.dateType,calendar.getTime());
        }

        int day = TimeUtil.getDateInfo(time,DateEnum.DAY);
        if(type.equals(DateEnum.DAY)){
            calendar.set(year,month-1,day);

            return new DateDimension(year,season,month,week,day,type.dateType,calendar.getTime());
        }

        throw new RuntimeException("指标解析错误");
    }

}
