package com.angus.app.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @author ：Angus
 * @date ：Created in 2022/4/24 15:05
 * @description：
 */
public class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        // TODO 1.创建JSON对象用于存储最终数据
        JSONObject jsonObject = new JSONObject();
        // TODO 2.获取库名，表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];

        // TODO 3.获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)){
            type = "insert";
        }

        // TODO 4.获取“before”数据 (如果是update操作才有before数据)
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();

        // TODO 先判断before数据是否存在
        if (before!=null){
            Schema beforeSchema = before.schema();
            List<Field> fieldList = beforeSchema.fields();
            for (Field field : fieldList) {
                Object beforeValue = before.get(field);
                beforeJson.put(field.name(), beforeValue);
            }

        }
        // TODO 5.获取“after”数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        // TODO 先判断before数据是否存在
        if (after!=null){
            Schema afterSchema = after.schema();
            List<Field> fieldList = afterSchema.fields();
            for (Field field : fieldList) {
                Object afterValue = after.get(field);
                afterJson.put(field.name(), afterValue);
            }
        }

        // TODO 6.将字段写入Json对象
        jsonObject.put("database",database);
        jsonObject.put("tableName", tableName);
        jsonObject.put("before", beforeJson);
        jsonObject.put("after", afterJson);
        jsonObject.put("type", type);

        // TODO 7.输出数据
        collector.collect(jsonObject.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
