package com.phone.etl.ip;

import com.phone.etl.common.EventLogsConstant;
import org.apache.commons.lang.StringUtils;

import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName LogUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description //TODO $
 **/
public class LogUtil {


    public static Map<String,String> parserLog(String log)  {

        Map<String,String> info = new ConcurrentHashMap<String,String>();

        if(StringUtils.isNotEmpty(log)){
            String[] words = log.split(EventLogsConstant.COLUMN_NAME_SEPARTOR);

            if(words.length == 4){
                info.put(EventLogsConstant.EVENT_COLUMN_NAME_IP,words[0]);
                info.put(EventLogsConstant.EVENT_COLUMN_NAME_SERVER_TIME,words[1].replaceAll("\\.",""));

                int index = words[3].indexOf("?");
                if(index>0){
                    String params = words[3].substring(index + 1);


                    handleParams(info,params);

                    handleIp(info);

                    handleUserAgent(info);

                }
            }

        }
        return info;
    }

    private static void handleUserAgent(Map<String, String> info) {
        if(info.containsKey(EventLogsConstant.EVENT_COLUMN_NAME_USERAGENT)){
            UserAgentUtil.UserAgentInfo userAgent = new UserAgentUtil().parserUserAgent(info.get(EventLogsConstant.EVENT_COLUMN_NAME_USERAGENT));

            if(userAgent != null){
                info.put(EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_NAME,userAgent.getBrowerName());
                info.put(EventLogsConstant.EVENT_COLUMN_NAME_BROWSER_VERSION,userAgent.getBrowerVersion());
                info.put(EventLogsConstant.EVENT_COLUMN_NAME_OS_NAME,userAgent.getOsName());
                info.put(EventLogsConstant.EVENT_COLUMN_NAME_OS_VERSION,userAgent.getOsVersion());
            }
        }


    }

    private static void handleIp(Map<String, String> info)  {
        if(info.containsKey(EventLogsConstant.EVENT_COLUMN_NAME_IP)){
            IpUtil.RegionInfo region = new IpUtil().getRegionInfoByIp(info.get(EventLogsConstant.EVENT_COLUMN_NAME_IP));


            if(region != null){
                info.put(EventLogsConstant.EVENT_COLUMN_NAME_COUNTRY,region.getCountry());
                info.put(EventLogsConstant.EVENT_COLUMN_NAME_PROVINCE,region.getProvince());
                info.put(EventLogsConstant.EVENT_COLUMN_NAME_CITY,region.getCity());
            }
        }


    }

    private static void handleParams(Map<String,String> info,String params){
        if(StringUtils.isNotEmpty(params)){
            String[] paramskeys = params.split("&");
            try{
                for (String kv : paramskeys){
                    String[] kvs = kv.split("=");
                    String k = kvs[0];
                    String v = URLDecoder.decode(kvs[1],"utf-8");

                    if(StringUtils.isNotEmpty(k)){
                        info.put(k,v);
                    }


                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}