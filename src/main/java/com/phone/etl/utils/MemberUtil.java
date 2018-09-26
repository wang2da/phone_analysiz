package com.phone.etl.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Auther: lyd
 * @Date: 2018/8/1 11:02
 * @Description:查看会员Id是否是新增会员，建议过滤不合法的会员id
 */
public class MemberUtil {
    private static Map<String,Boolean> cache = new LinkedHashMap<String,Boolean>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return this.size() > 1000;
        }
    };

    /**
     * 检测会员ID是否合法
     * @param memeberid
     * @return   true是合法，false不合法
     */
    public static boolean checkMemberId(String memeberid){
        String regex = "^[0-9a-zA-Z].*$";
        if(StringUtils.isNotEmpty(memeberid)){
            return memeberid.trim().matches(regex);
        }
        return false;
    }

    /**
     * 是否是一个新增的会员
     * @param memberId
     * @param conn
     * @param conf
     * @return  true是新增会员，false不是新增会员
     */
    public static boolean isNewMember(String memberId, Connection conn, Configuration conf){

        PreparedStatement ps = null;
        ResultSet rs = null;
        Boolean res = false;
        try {
            res = cache.get(memberId);
            if(res == null){
                String sql = conf.get("total_member_info");
                ps = conn.prepareStatement(sql);
                ps.setString(1,memberId);
                rs = ps.executeQuery();
                if(rs.next()){
                    res = false;
                } else {
                    res = true;
                }
                //添加到cache中
                cache.put(memberId,res);

            }
        } catch (SQLException e){
            e.printStackTrace();
        }
        return res == null ? false : res.booleanValue();
    }
}