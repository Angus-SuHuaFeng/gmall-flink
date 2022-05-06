package com.angus.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author ：Angus
 * @date ：Created in 2022/5/3 21:11
 * @description：
 */
public interface DimAsyncJoinFunction<T> {
    void join(T input, JSONObject dimInfo) throws ParseException;
    String getKey (T input);
}
