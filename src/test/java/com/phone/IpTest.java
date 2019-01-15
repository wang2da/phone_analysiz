package com.phone;

import com.phone.etl.ip.IpUtil;
import com.phone.etl.ip.IPSeeker;

/**
 * @ClassName IpTest
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description //TODO $
 **/
public class IpTest {
    public static void main(String[] args) {
        System.out.println(IPSeeker.getInstance().getCountry("43.47.150.72"));

        System.out.println(IpUtil.getRegionInfoByIp("43.47.150.72"));

        try {
            System.out.println(IpUtil.parserIp1("http://ip.taobao.com/service/getIpInfo.php?ip=43.47.150.72","utf-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}