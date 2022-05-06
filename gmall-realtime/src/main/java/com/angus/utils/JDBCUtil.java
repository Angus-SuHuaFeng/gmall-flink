package com.angus.utils;

import com.alibaba.fastjson.JSONObject;
import com.angus.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.codehaus.jackson.map.util.BeanUtil;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ：Angus
 * @date ：Created in 2022/5/2 16:01
 * @description：
 */
public class JDBCUtil {
    // TODO 获取连接
//    public static Connection getConnection(String driver, String url) {
//
//    }

    // TODO 查询多条数据
    public static <T> List<T> queryList(Connection connection, String query, Class<T> tClass, boolean underScoreToCamel){
        // TODO 1.创建集合用于存放查询结果
        ArrayList<T> resultList = new ArrayList<>();
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        // TODO 2.执行SQL
        try {
            // TODO 预编译
            preparedStatement = connection.prepareStatement(query);
            // TODO 查询
            resultSet = preparedStatement.executeQuery();
            // TODO 将查询结果放到resultList中
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()){
                // TODO 创建对象泛型对
                T t = tClass.newInstance();
                // TODO 给泛型对赋值  jdbc中都是从1开始
                for (int i = 1; i < columnCount + 1; i++) {
                    // TODO 获取列名
                    String columnName = metaData.getColumnName(i);
                    // TODO 判断是否需要转换驼峰命名
                    if (underScoreToCamel) {
                        columnName = CaseFormat.LOWER_UNDERSCORE
                                .to(CaseFormat.LOWER_CAMEL, columnName.toLowerCase());
                    }
                    // TODO 获取列值
                    Object value = resultSet.getObject(i);
                    // TODO 给泛型对象赋值
                    BeanUtils.setProperty(t,columnName,value);
                }
                resultList.add(t);
            }
        } catch (SQLException e) {
            throw new RuntimeException("执行SQL" + query + "异常");
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }finally {
            try {
                if (preparedStatement!=null) {
                    preparedStatement.close();
                }
                if (resultSet != null) {
                    resultSet.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        // TODO 3.返回结果集合
        return resultList;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // TODO TEST
//        Class.forName(GmallConfig.PHOENIX_DRIVER);
//        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
//        List<JSONObject> jsonObjects = queryList(connection,
//                "SELECT * FROM GMALL_REALTIME.DWD_USER_INFO",
//                JSONObject.class,
//                true);
//        for (JSONObject jsonObject : jsonObjects) {
//            System.out.println(jsonObject.toJSONString());
//        }
//        connection.close();
    }
}
