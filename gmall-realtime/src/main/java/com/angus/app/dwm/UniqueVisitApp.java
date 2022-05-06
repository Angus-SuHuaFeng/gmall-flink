package com.angus.app.dwm;

import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.angus.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

 /**
 * @author ：Angus
 * @date ：Created in 2022/4/28 22:31
 * @description：dwd_page_log
  *  数据：SpringBoot -> nginx -> SpringBoot -> FlinkCDC -> Kafka(ods_base_log) -> FlinkApp -> Kafka(dwd_page_log) -> FlinkApp -> Kafka
  *  程序：gmallapp -> nginx -> logger -> FlinkCDC -> Kafka(ods_base_log) -> BaseLogApp -> Kafka -> UniqueVisitApp -> kafka(dwm_unique_visit)
 */
public class UniqueVisitApp {
     private static final OutputTag<String> dirtyOutputTag = new OutputTag<String>("dirty"){};
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

        // TODO 2.读取Kafka数据 dwd_page_log
        String sourceTopic = "dwd_page_log";
        String groupId = "dwd_page_group";
        String sinkTopic = "dwm_unique_visit";
        DataStreamSource<String> pageStream = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));
        // TODO 3.将数据转换成JSON并添加WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageStream.process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector)  {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(s);
                            collector.collect(jsonObject);
                        }catch (Exception e){
                            context.output(dirtyOutputTag, s);
                        }
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(100))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject s, long l) {
                                return s.getLong("ts");
                            }
                        }));
//        pageStream.print("page");
//        jsonObjStream.print("json");
//        jsonObjStream.getSideOutput(dirtyOutputTag).print("dirty");
        // TODO 4.过滤数据，状态编程，只保留每个mid每天第一次登录时候的数据
        KeyedStream<JSONObject, String> keyedStream = jsonObjStream.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> filterStream = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            ValueState<String> tsState;
            SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> ts_stateDescriptor = new ValueStateDescriptor<>("ts_state", String.class);
                // 状态可以设置TTL（超时时间）
                StateTtlConfig ttlConfig = new StateTtlConfig
                        .Builder(Time.milliseconds(24 * 60 * 60 * 1000L - (new Date().getTime()%24 * 60 * 60 * 1000L)))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                ts_stateDescriptor.enableTimeToLive(ttlConfig);
                tsState = getRuntimeContext().getState(ts_stateDescriptor);
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                // TODO 取出当前数据的上次浏览的页面 （如果为null则说明是第一次打开App）
                String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");
                // TODO 判断上一次浏览的页面是否为空，不为空直接过滤
                if (lastPage == null || lastPage.length() <= 0) {
                    // TODO 获取状态
                    String midTsState = tsState.value();
                    // TODO 判断状态中的值和来的数据是否相等
                    String currTs = simpleDateFormat.format(jsonObject.getLong("ts"));
                    if (!currTs.equals(midTsState)) {
                        tsState.update(currTs);
                        return true;
                    }
                }
                return false;
            }
        });
        filterStream.print();
        // TODO 5.将数据写入Kafka
        filterStream.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaProducer(sinkTopic));

        // TODO 6.启动程序
        env.execute("dwm_unique_visit_App");
    }
}
