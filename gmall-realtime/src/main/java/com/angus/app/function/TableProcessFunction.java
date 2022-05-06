package com.angus.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.angus.bean.TableProcess;
import com.angus.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;

import static com.angus.app.function.TableOperator.createHbaseTable;
import static com.angus.app.function.TableOperator.filterColumn;

/**
 * @author ：Angus
 * @date ：Created in 2022/4/25 23:13
 * @description：
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String , JSONObject> {
    private Connection connection;
    private OutputTag<JSONObject> hbaseOutputTag;
    private MapStateDescriptor<String , TableProcess> mapStateDescriptor = new MapStateDescriptor<String, TableProcess>("map-state",String.class,TableProcess.class);

    public TableProcessFunction(OutputTag<JSONObject> hbaseOutputTag) {
        this.hbaseOutputTag = hbaseOutputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

    }

    @Override
    public void processElement(JSONObject kafkaObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        // TODO 1.获取状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        String key = kafkaObject.getString("tableName") + "-" + kafkaObject.getString("type");
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess!=null){
            // TODO 2.过滤字段（配置表）
            JSONObject after = kafkaObject.getJSONObject("after");
            // TODO 3.分流
            kafkaObject.put("sinkTable", tableProcess.getSinkTable());
            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                // kafka 主流直接写入
                collector.collect(kafkaObject);
            }else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                filterColumn(after, tableProcess.getSinkColumns());
                // hbase 写入侧输出流
                readOnlyContext.output(hbaseOutputTag, kafkaObject);
            }
        }else {
//            System.out.println("不存在组合" + key);
        }

    }

    // TODO 广播流
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        // TODO 1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        String after = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(after, TableProcess.class);
        // TODO 2.建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
            createHbaseTable(connection, tableProcess.getSinkTable(), tableProcess.getSinkColumns(), tableProcess.getPrimaryKey(), tableProcess.getSinkExtend());
        }
        // TODO 3.写入状态
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key, tableProcess);
    }
}
