package com.phone.etl.utils;

/**
 * @ClassName KpiType
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 统计指标的枚举
 **/
public enum KpiType {
    NEW_USER("new_user"),
    BROWSER_NEW_USER("browser_new_user"),
    ACTIVE_USER("active_user"),
    BROWSER_ACTIVE_USER("browser_active_user"),
    MEMBER_USER("member_user"),
    BROWSER_MEMBER_USER("browser_member_user"),
    NEW_MEMBER("new_member"),
    BROWSER_NEW_MEMBER("browser_new_member"),
    MEMBER_INFO("member_info"),
    SESSIONS("sessions"),
    PAGEVIEW("page_view")
    ;

    public String kpiName;

    KpiType(String kpiName) {
        this.kpiName = kpiName;
    }

    /**
     * 根据kpi的name获取对应的指标
     * @param name
     * @return
     */
    public static KpiType valueOfKpiName(String name){
        for (KpiType kpi : values()){
            if(kpi.kpiName.equals(name)){
                return kpi;
            }
        }
        return null;
    }

    /**
     * 根据kpi的name获取kpi的枚举
     * @param kpiName
     * @return
     */
    public static KpiType valueOfType(String kpiName){
        for (KpiType kpi : values()){
            if(kpiName.equals(kpi.kpiName)){
                return kpi;
            }
        }
        throw  new RuntimeException("该kpiName暂不支持获取kpi的枚举."+kpiName);
    }

}
