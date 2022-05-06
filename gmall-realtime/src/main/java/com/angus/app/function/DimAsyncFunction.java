package com.angus.app.function;

import com.alibaba.fastjson.JSONObject;
import com.angus.common.GmallConfig;
import com.angus.utils.DimUtil;
import com.angus.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author ：Angus
 * @date ：Created in 2022/5/2 22:59
 * @description：
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncJoinFunction<T>{
    private Connection connection = null;
    ThreadPoolExecutor threadPool = null;

    private final String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPool = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPool.submit(new Runnable() {
            @Override
            public void run() {
                // TODO 1.获取查询的维度表主键
                String id = getKey(input);
                // TODO 2.查询维度信息
                JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, id);
                // TODO 3.补充维度信息
                if (dimInfo != null && dimInfo.size() > 0) {
                    try {
                        join(input, dimInfo);
                    }catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                // TODo 4.将数据输出
                resultFuture.complete(Collections.singletonList(input));
            }
        });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut: " + input);
    }
}
