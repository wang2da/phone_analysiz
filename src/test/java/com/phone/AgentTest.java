package com.phone;

import com.phone.etl.ip.UserAgentUtil;

public class AgentTest {
    public static void main(String[] args) {
        System.out.println(UserAgentUtil.parserUserAgent("Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.6 Safari/537.36"));
    }
}
