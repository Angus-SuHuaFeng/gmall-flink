package com.angus.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.angus.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author ：Angus
 * @date ：Created in 2022/4/30 18:18
 * @description：
 */
public class UserJumpDetailApp {

    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(4);  // 生产环境默认为Kafka分区数
        // TODO 1.1 开启checkPoint和状态后端
//        env.setStateBackend(new FsStateBackend(new Path("hdfs://hadoop1:8020/gmall-flink/checkPoint")));
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // TODO 2.读取Kafka数据
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // TODO 3.将每行数据转换成Json对象并提取时间戳生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, Context context, Collector<JSONObject> collector) {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(new OutputTag<String>("dirty") {
                    }, s);
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long l) {
                        return element.getLong("ts");
                    }
                }));
        // TODO 4.CEP编程
        // TODO 4.1 定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern
                .<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return (lastPageId == null || lastPageId.length() <= 0);
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return (lastPageId == null || lastPageId.length() <= 0);
            }
        }).within(Time.seconds(10));

        // TODO 4.2 将模式序列作用到流上
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        PatternStream<JSONObject> patternStream = CEP.pattern(
                keyedStream,
                pattern);
        // TODO 4.3 提取匹配上的和超时事件
        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.process(new JumpDetailMatch());
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("timeout"){};
        DataStream<JSONObject> timeOutputDS = selectDS.getSideOutput(timeOutTag);
        // TODO 5.UNION俩个事件
        DataStream<JSONObject> unionDS = selectDS.union(timeOutputDS);
//
//        // TODO 6.将数据写入Kafka
        unionDS.print();

        unionDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));
        selectDS.print("selected>>>");
        timeOutputDS.print("timeOutput>>>");
        // TODO 7.执行程序
        env.execute("UserJumpDetail");
    }
    // TODO 自定义PatternProcessFunction
    private static class JumpDetailMatch extends PatternProcessFunction<JSONObject, JSONObject> implements TimedOutPartialMatchHandler<JSONObject> {

        @Override
        public void processMatch(Map<String, List<JSONObject>> map, Context context, Collector<JSONObject> collector) throws Exception {
            JSONObject start = map.get("start").get(0);
            collector.collect(start);
        }

        @Override
        public void processTimedOutMatch(Map<String, List<JSONObject>> map, Context context) throws Exception {
            JSONObject start = map.get("start").get(0);
            context.output(new OutputTag<JSONObject>("timeout"){},start);
        }
    }
}
