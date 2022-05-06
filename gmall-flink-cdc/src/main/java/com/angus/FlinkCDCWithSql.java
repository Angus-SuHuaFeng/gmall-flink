package com.angus;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author ：Angus
 * @date ：Created in 2022/4/24 1:10
 * @description：
 */
public class FlinkCDCWithSql {
    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.DDL方式建表
        String createDDL = "CREATE TABLE mysql_binlog (" +
                "    id STRING NOT NULL," +
                "    tm_name STRING," +
                "    logo_url STRING " +
                ") WITH (" +
                "    'connector' = 'mysql-cdc'," +
                "    'hostname' = 'hadoop1'," +
                "    'port' = '3306'," +
                "    'username' = 'root'," +
                "    'password' = '000000'," +
                "    'database-name' = 'gmall'," +
                "    'table-name' = 'base_trademark'" +
                ")";
        tableEnv.executeSql(createDDL);

        // TODO 3.查询数据
        Table sqlQuery = tableEnv.sqlQuery("select * from mysql_binlog");

        // TODO 4.将动态表转换成流
        DataStream<Tuple2<Boolean, Row>> toRetractStream = tableEnv.toRetractStream(sqlQuery, Row.class);
        toRetractStream.print("Retract");


        // TODO 5.启动任务
        env.execute("SQL_CDC");
    }
}
