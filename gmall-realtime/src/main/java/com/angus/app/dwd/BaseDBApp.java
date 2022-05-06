package com.angus.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.angus.app.function.DimSinkFunction;
import com.angus.app.function.JsonDebeziumDeserializationSchema;
import com.angus.app.function.TableProcessFunction;
import com.angus.bean.TableProcess;
import com.angus.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;

import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

 /**
 * @author ：Angus
 * @date ：Created in 2022/4/25 21:38
 * @description：
  *  数据：SpringBoot -> nginx -> SpringBoot -> FlinkCDC -> Kafka(ods_base_db) -> FlinkApp -> Kafka
  *  程序：gmallapp -> nginx -> logger -> FlinkCDC -> Kafka(ods_base_db) -> BaseDBApp -> Kafka
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO 1.1 开启checkPoint和状态后端
//        env.setStateBackend(new FsStateBackend(new Path("hdfs://hadoop1:8020/gmall-flink/checkPoint")));
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // TODO 2.消费Kafka ods_base_db  主题数据
        String topic = "ods_base_db";
        String groupId = "base_db_app";


        // TODO 4.使用FlinkCDC消费配置表并处理成广播流
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop1")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall-realtime")
                .tableList("gmall-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> tableProcessDStream = env.addSource(mysqlSource);
        tableProcessDStream.print("tableProcessDStream");
        MapStateDescriptor<String, TableProcess> processMapStateDescriptor = new MapStateDescriptor<String, TableProcess>(
                "map-state", String.class, TableProcess.class);

        BroadcastStream<String> broadcastStream = tableProcessDStream.broadcast(processMapStateDescriptor);

        DataStreamSource<String> kafkaStream = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 3.将每行数据转换成JSON对象并过滤（delete）
        SingleOutputStreamOperator<JSONObject> jsonStream = kafkaStream.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String type = jsonObject.getString("type");
                        return !"delete".equals(type);
                    }
                });



        // TODO 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonStream.connect(broadcastStream);

        // TODO 6.分流，处理数据，广播流数据，主流数据（根据广播流数据进行处理）
        OutputTag<JSONObject> hbaseOutTag = new OutputTag<JSONObject>("hbase") {};
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseOutTag));

        // TODO 7.提取Kafka流和HBase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseOutTag);

        // TODO 8.将Kafka流数据写入Kafka主题，将数据写入Phoenix表
        kafka.print("kafka>>>");
        hbase.print("hbase>>>");

        hbase.addSink(new DimSinkFunction());
        kafka.addSink(MyKafkaUtil.getKafkaProducer(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<>(element.getString("sinkTable"), element.getString("after").getBytes(StandardCharsets.UTF_8));
            }
        }));
        // TODO 9.启动任务
        env.execute("ODS->DWD_KAFKA/DIM_HBASE");
    }
}
