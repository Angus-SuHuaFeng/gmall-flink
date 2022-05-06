package com.angus.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.angus.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

 /**
 * @author ：Angus
 * @date ：Created in 2022/4/24 17:51
 * @description：
  *  数据：SpringBoot -> nginx -> SpringBoot -> FlinkCDC -> Kafka(ods_base_log) -> FlinkApp -> Kafka
  *  程序：gmallapp -> nginx -> logger -> FlinkCDC -> Kafka -> BaseLogApp -> Kafka
 */
public class BaseLogApp {
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

        // TODO 2.消费ods_base_log主题的数据流
        String topic = "ods_base_log";
        String groupId = "base_log_app";

        SingleOutputStreamOperator<String> kafkaStream = env.addSource(MyKafkaUtil.getKafkaConsumer(topic, groupId));

        // TODO 3.每行数据转换成JSON对象
        OutputTag<String> dirtyOutputTag = new OutputTag<String>("Dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonStream = kafkaStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    // 如果JSON解析有异常，将异常数据输出到侧输出流
                    context.output(dirtyOutputTag, value);
                }
            }
        });

        // TODO 4.新老用户校验（状态编程）
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagStream = jsonStream.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"))
                .map(new RichMapFunction<JSONObject, JSONObject>() {

                    private ValueState<String> userState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        userState = getRuntimeContext().getState(new ValueStateDescriptor<String>("new-state", String.class));
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        // TODO 获取数据中的is_new标记
                        String is_new = value.getJSONObject("common").getString("is_new");
                        if ("1".equals(is_new)) {
                            // TODO 如果is_new为1，则进一步检验状态是否为新用户
                            if (userState != null) {
                                // 修改is_new
                                value.getJSONObject("common").put("is_new", "0");
                            } else {
                                userState.update("0");
                            }
                        }
                        return value;
                    }
                });


        // TODO 5.分流  测输出流  主流： 页面数据   侧输出流：启动， 曝光     ###侧输出流要使用匿名类，否则泛型擦除
        OutputTag<String> startOutputTag = new OutputTag<String>("start"){};
        OutputTag<String> displaysOutputTag = new OutputTag<String>("displays"){};
        SingleOutputStreamOperator<String> pageStream = jsonWithNewFlagStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context context, Collector<String> out) throws Exception {
                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    context.output(startOutputTag, value.toJSONString());
                } else {
                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {
                        // 将曝光数据写入侧输出流

                        // 获取页面ID
                        String pageId = value.getJSONObject("page").getString("page_id");

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displaysJSONObject = displays.getJSONObject(i);
                            displaysJSONObject.put("page_id", pageId);
                            context.output(displaysOutputTag, displaysJSONObject.toJSONString());
                        }

                    }
                    // 将数据写入以页面日志主流
                    value.remove("displays");
                    out.collect(value.toJSONString());
                }
            }
        });

        // TODO 6.提取侧输出流
        DataStream<String> startSideOutput = pageStream.getSideOutput(startOutputTag);
        DataStream<String> displaySideOutput = pageStream.getSideOutput(displaysOutputTag);
        DataStream<String> dirtySideOutput = jsonStream.getSideOutput(dirtyOutputTag);
        // TODO 7.将三个流输出到对应的Kafka主题中
//        startSideOutput.print("START>>>>>>>>>>");
//        pageStream.print("PAGE>>>>>>>>>>>");
//        displaySideOutput.print("DISPLAY>>>>>>>>>>>");
//        dirtySideOutput.print("脏数据>>>>>>>>>>>");
        startSideOutput.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        pageStream.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        displaySideOutput.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));
        dirtySideOutput.addSink(MyKafkaUtil.getKafkaProducer("dirty_log"));

        // TODO 8.启动任务
        env.execute("BaseLogApp");
    }
}
