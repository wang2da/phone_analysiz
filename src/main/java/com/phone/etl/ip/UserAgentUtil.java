package com.phone.etl.ip;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import sun.rmi.runtime.Log;

import java.io.IOException;

/**
 * @ClassName UserAgentUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description
 *
 * window.navigator.userAgent
 **/
public class UserAgentUtil {

    public static final Logger logger = Logger.getLogger(UserAgentUtil.class);

    private static UserAgentInfo info = new UserAgentInfo();

    private static UASparser uaSparser = null;
    static {
        try {
            uaSparser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            logger.error("创建Uasparser失败....",e);
        }
    }

    public static UserAgentInfo parserUserAgent(String userAgent){
        if(StringUtils.isEmpty(userAgent))
            return null;

        try {
            cz.mallat.uasparser.UserAgentInfo ua = uaSparser.parse(userAgent);
            if(ua != null){
                info.setBrowerName(ua.getUaFamily());
                info.setBrowerVersion(ua.getBrowserVersionInfo());
                info.setOsName(ua.getOsName());
                info.setOsVersion(ua.getOsFamily());
            }
        } catch (IOException e) {
            logger.warn("解析异常....");
        }
        return info;
    }

    /**
     * 用于封装useragent解析后的信息
     */
    public static class UserAgentInfo{
        private String browerName;
        private String browserVersion;
        private String osName;
        private String osVersion;

        public String getBrowerName() {
            return browerName;
        }

        public void setBrowerName(String browerName) {
            this.browerName = browerName;
        }

        public String getBrowerVersion() {
            return browserVersion;
        }

        public void setBrowerVersion(String browerVersion) {
            this.browserVersion = browerVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browerName='" + browerName + '\'' +
                    ", browerVersion='" + browserVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }
    }
}