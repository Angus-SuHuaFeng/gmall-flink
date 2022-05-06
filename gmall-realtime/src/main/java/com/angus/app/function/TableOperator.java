package com.angus.app.function;

import com.alibaba.fastjson.JSONObject;
import com.angus.common.GmallConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author ：Angus
 * @date ：Created in 2022/4/26 15:26
 * @description：
 */
public class TableOperator{

    // TODO :  CREATE TABLE IF NOT EXISTS
    public static void createHbaseTable(Connection connection, String sinkTable, String sinkColumns, String primaryKey, String sinkExtend){
        PreparedStatement preparedStatement = null;

        primaryKey = primaryKey==null?"id":primaryKey;
        sinkExtend = sinkExtend==null?"":sinkExtend;

        StringBuffer createTableSql = new StringBuffer("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            // 判断是否为主键
            if (field.equals(primaryKey)){
                createTableSql.append(field).append(" varchar primary key ");
            }else {
                createTableSql.append(field).append(" varchar ");
            }
            // 判断是否为最后一个字段，不是加',' 是不管
            if (i < fields.length-1){
                createTableSql.append(",");
            }
        }
        createTableSql.append(")").append(sinkExtend);

        // TODO 打印建表语句
//        System.out.println(createTableSql);
        try {
        // TODO 执行SQL
            preparedStatement = connection.prepareStatement(createTableSql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Phoenix" + "创建表" + sinkTable +"失败!");
        }finally {
            if (preparedStatement!=null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static void filterColumn(JSONObject data, String sinkColumns){
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);

        // 移除不在配置表中的变化流，例如评论表，我们不进行处理，所以肯定也不在配置表中，这里就可以移除
//        while (iterator.hasNext()){
//            Map.Entry<String, Object> next = iterator.next();
//            if (!columns.contains(next.getKey())){
//                iterator.remove();
//            }
//        }
        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));
    }

}
