package com.angus.utils;

import com.alibaba.fastjson.JSONObject;
import com.angus.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * @author ：Angus
 * @date ：Created in 2022/5/2 17:31
 * @description：
 */
public class DimUtil {
    public static JSONObject getDimInfo(Connection connection, String tableName, String id){

        // TODO 查询Phoenix之前先去查询Redis
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null) {
            // 重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            // 关闭连接
            jedis.close();
            return JSONObject.parseObject(dimInfoJsonStr);
        }

        // TODO 拼接查询语句 (注意单引号 ' )
        String querySQL = "SELECT * FROM " + GmallConfig.HBASE_SCHEMA + "." + tableName + " WHERE id='" + id + "'";

        // TODO 查询Phoenix
        List<JSONObject> queryList = JDBCUtil.queryList(connection, querySQL, JSONObject.class, true);
        JSONObject dimInfoJson = queryList.get(0);
        // TODO 在返回结果之前先写入Redis
        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24*60*60);
        jedis.close();
        return dimInfoJson;
    }

    public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        long start = System.currentTimeMillis();
        JSONObject dim_base_trademark = getDimInfo(connection, "DIM_USER_INFO", "1100");
        long end = System.currentTimeMillis();
        JSONObject dim_base_trademark1 = getDimInfo(connection, "DIM_USER_INFO", "1100");
        long end1 = System.currentTimeMillis();
        JSONObject dim_base_trademark2 = getDimInfo(connection, "DIM_USER_INFO", "1100");
        long end2 = System.currentTimeMillis();
        System.out.println(dim_base_trademark);
        System.out.println(end-start);
        System.out.println(dim_base_trademark1);
        System.out.println(end1-end);
        System.out.println(dim_base_trademark2);
        System.out.println(end2-end1);

    }
}
