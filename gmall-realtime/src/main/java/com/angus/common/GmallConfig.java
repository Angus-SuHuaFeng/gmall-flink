package com.angus.common;

/**
 * @author ：Angus
 * @date ：Created in 2022/4/25 23:18
 * @description：
 */
public class GmallConfig {
    // Phoenix 库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";

    // Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix 连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop1,hadoop2,hadoop3:2181";


}
