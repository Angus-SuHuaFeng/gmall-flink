package com.angus.app.function;

import com.alibaba.fastjson.JSONObject;
import com.angus.common.GmallConfig;
import com.angus.utils.DimUtil;
import com.angus.utils.RedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author ：Angus
 * @date ：Created in 2022/4/27 11:37
 * @description：
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    // Value:
    // {"sinkTable":"dim_base_trademark","database":"gmall",
    // "before":{"tm_name":"1","id":12},"after":{"tm_name":"111","id":12},"type":"update","tableName":"base_trademark"}

    // SQL: upsert into db.tb(id,tm_name) values()
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        String upsertSQL = null;
        try {
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            // TODO 获取SQL语句
            upsertSQL = getUpsertQuery(sinkTable, after);
            // TODO 预编译
            preparedStatement = connection.prepareStatement(upsertSQL);
            // TODO 如果执行的是更新操作，先删除Redis数据
            if ("update".equals(value.getString("type"))){
                DimUtil.delRedisDimInfo(sinkTable.toUpperCase(), after.getString("id"));
            }
            // TODO 执行
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("执行" + upsertSQL + "失败!");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

    }
    // SQL: upsert into db.tb(id,tm_name) values()
    private String getUpsertQuery(String sinkTable, JSONObject after) {
        // "after":{"tm_name":"111","id":12}
        // keySet() 获取json对象的key值的集合   'tm_name'，'id'
        Set<String> keySet = after.keySet();
        // 获取values   '111'， '12'
        Collection<Object> values = after.values();
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "( " +
                StringUtils.join(keySet, ",") + ") values ('" +
                StringUtils.join(values, "','") + "')" ;
    }
}
