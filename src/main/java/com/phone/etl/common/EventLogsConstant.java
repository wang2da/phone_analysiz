package com.phone.etl.common;

public class EventLogsConstant {

    public static enum EventEnum{
        LAUNCH(1,"lanuch_event","e_l"),
        PAGEVIEW(2,"page_view","e_pv"),
        CHARGEREQUEST(3,"charge_request_event","e_crt"),
        CHARGESUCCESS(4,"charge_success","e_cs"),
        CHARGEREFUND(5,"charge_refund","e_cr"),
        EVENT(6,"event","e_e");

        public int id;
        public String name;
        public String alias;

        EventEnum(int id, String name, String alias) {
            this.id = id;
            this.name = name;
            this.alias = alias;
        }

        public static EventEnum valueOfAlias(String alias){
            for(EventEnum event : values()){
                if(alias.equals(event.alias)){
                    return event;
                }

            }
            return null;
        }

    }

    public static final String HBASE_TABLE_NAME = "logs";
    public static final String HBASE_COLUMN_FAMILY = "info";
    public static final String EVENT_COLUMN_NAME_VERSION="ver";
    public static final String EVENT_COLUMN_NAME_SERVER_TIME="s_time";
    public static final String EVENT_COLUMN_NAME_EVENT_NAME="en";
    public static final String EVENT_COLUMN_NAME_UUID = "u_uid";
    public static final String EVENT_COLUMN_NAME_MERMBER_ID = "u_mid";
    public static final String EVENT_COLUMN_NAME_SESSION_ID = "u_sd";
    public static final String EVENT_COLUMN_NAME_SESSION_CLIENT_TIME = "c_time";

    public static final String EVENT_COLUMN_NAME_LANGUAGE="l";
    public static final String EVENT_COLUMN_NAME_USERAGENT="b_iev";
    public static final String EVENT_COLUMN_NAME_RESOLUTION="b_rst";
    public static final String EVENT_COLUMN_NAME_CURRENT_URL="p_ref";
    public static final String EVENT_COLUMN_NAME_TITLE="tt";
    public static final String EVENT_COLUMN_NAME_PLATFORM="pl";
    public static final String EVENT_COLUMN_NAME_IP="ip";
    public static final String COLUMN_NAME_SEPARTOR="\\^A";


    public static final String EVENT_COLUMN_NAME_ORDER_ID="oid";
    public static final String EVENT_COLUMN_NAME_ORDER_NAME="on";
    public static final String EVENT_COLUMN_NAME_CURRENCY_AMOUTN="cua";
    public static final String EVENT_COLUMN_NAME_CURRENCY_TYPE="cut";
    public static final String EVENT_COLUMN_NAME_PAYMENT_TYPE="pt";

    public static final String EVENT_COLUMN_NAME_EVENT_CATEGORY="ca";
    public static final String EVENT_COLUMN_NAME_EVENT_ACTION="ac";
    public static final String EVENT_COLUMN_NAME__EVENT_KV="kv_";
    public static final String EVENT_COLUMN_NAME_EVENT_DURATION="du";



    public static final String EVENT_COLUMN_NAME_BROWSER_NAME="browserName";
    public static final String EVENT_COLUMN_NAME_BROWSER_VERSION="browserVersion";
    public static final String EVENT_COLUMN_NAME_OS_NAME="osName";
    public static final String EVENT_COLUMN_NAME_OS_VERSION="osVersion";

    public static final String EVENT_COLUMN_NAME_COUNTRY="country";
    public static final String EVENT_COLUMN_NAME_PROVINCE="province";
    public static final String EVENT_COLUMN_NAME_CITY="city";
}
