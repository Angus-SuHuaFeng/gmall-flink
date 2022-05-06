package com.angus.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.angus.app.function.DimAsyncFunction;
import com.angus.bean.OrderDetail;
import com.angus.bean.OrderInfo;
import com.angus.bean.OrderWide;
import com.angus.utils.MyKafkaUtil;
import org.apache.calcite.linq4j.Ord;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author ：Angus
 * @date ：Created in 2022/5/1 22:49
 * @description：
 */
public class OrderWideApp {
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
        // TODO 2.读取Kafka数据，并转换为JavaBean对象，提取时间戳生成WaterMark
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";
        SingleOutputStreamOperator<OrderInfo> orderInfoStream = env.addSource(MyKafkaUtil.getKafkaConsumer(orderInfoSourceTopic, groupId))
                .process(new ProcessFunction<String, OrderInfo>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, OrderInfo>.Context ctx, Collector<OrderInfo> out) throws Exception {
                        try {
                            OrderInfo orderInfo = JSON.parseObject(value, OrderInfo.class);
                            String[] dateTime = orderInfo.getCreate_time().split(" ");
                            orderInfo.setCreate_date(dateTime[0]);
                            orderInfo.setCreate_hour(dateTime[1].split(":")[0]);
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                            out.collect(orderInfo);
                        } catch (Exception e) {
                            ctx.output(new OutputTag<String>("dirty") {
                            }, value);
                        }
                    }
                });
        SingleOutputStreamOperator<OrderInfo> orderInfoStreamWithTimeStamp = orderInfoStream.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                        return orderInfo.getCreate_ts();
                    }
                }));
        SingleOutputStreamOperator<OrderDetail> orderDetailStreamWithTimeStamp = env.addSource(MyKafkaUtil.getKafkaConsumer(orderDetailSourceTopic, groupId))
                .process(new ProcessFunction<String, OrderDetail>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, OrderDetail>.Context ctx, Collector<OrderDetail> out) throws Exception {
                        try {
                            OrderDetail orderDetail = JSON.parseObject(value, OrderDetail.class);
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                            out.collect(orderDetail);
                        } catch (Exception e) {
                            ctx.output(new OutputTag<String>("dirty") {}, value);
                        }
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofMillis(2000))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long l) {
                                return orderDetail.getCreate_ts();
                            }
                        }));

        // TODO 3.双流Join
        SingleOutputStreamOperator<OrderWide> orderWideNoWithDimDS = orderInfoStreamWithTimeStamp.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailStreamWithTimeStamp.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-2), Time.seconds(2))        // 生产环境中为最大延迟时间
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
        // TODO 4.关联维度信息  Hbase phoenix, 这里用map效率太差，用异步IO优化
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideNoWithDimDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) throws ParseException{
                        // 关联性别属性
                        orderWide.setUser_gender(dimInfo.getString("gender"));
                        // 关联生日属性
                        String birthday = dimInfo.getString("birthday");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:dd");
                        try {
                            long birthdayTimeMillis = sdf.parse(birthday).getTime();
                            long currentTimeMillis = System.currentTimeMillis();
                            long age = (currentTimeMillis - birthdayTimeMillis) / (1000 * 60 * 60 * 24 * 365L);
                            orderWide.setUser_age((int) age);
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    }
                },
                65,
                TimeUnit.SECONDS);
        // TODO 4.2 关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserAndProvince = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setProvince_name(dimInfo.getString("name"));
                        orderWide.setProvince_area_code(dimInfo.getString("areaCode"));
                        orderWide.setProvince_iso_code(dimInfo.getString("isoCode"));
                        orderWide.setProvince_3166_2_code(dimInfo.getString("iso31662"));
                    }
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getProvince_id().toString();
                    }
                }, 60, TimeUnit.SECONDS);

        // TODO 4.3 关联SKU
        SingleOutputStreamOperator<OrderWide> orderWideWithUserAndProvinceAndSKU = AsyncDataStream.unorderedWait(orderWideWithUserAndProvince,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setSku_name(dimInfo.getString("skuName"));
                        orderWide.setCategory3_id(dimInfo.getLong("category3Id"));
                        orderWide.setSpu_id(dimInfo.getLong("spuId"));
                        orderWide.setTm_id(dimInfo.getLong("tmId"));
                    }
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getSku_id().toString();
                    }
                }, 60, TimeUnit.SECONDS);

        // TODO 4.4 关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserProvinceSKUSPU = AsyncDataStream.unorderedWait(orderWideWithUserAndProvinceAndSKU,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setSpu_name(dimInfo.getString("spuName"));
                    }
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getSpu_id().toString();
                    }
                }, 60, TimeUnit.SECONDS);
        // TODO 4.5 关联 Category维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory = AsyncDataStream.unorderedWait(orderWideWithUserProvinceSKUSPU,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfo) {
                        orderWide.setCategory3_name(dimInfo.getString("name"));
                    }
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getCategory3_id().toString();
                    }
                }, 60, TimeUnit.SECONDS);

        // TODO 4.6 关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS =
                AsyncDataStream.unorderedWait(
                        orderWideWithCategory, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK")
                        {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException{
                                orderWide.setTm_name(jsonObject.getString("tmName"));
                            }
                            @Override
                            public String getKey(OrderWide orderWide) {
                                return orderWide.getTm_id().toString();
                            }
                        }, 60, TimeUnit.SECONDS);
        orderWideWithTmDS.print("orderWide>>>");
        // TODO 5.将订单宽表写入Kafka
        orderWideWithTmDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaProducer(orderWideSinkTopic));
        // TODO 6.启动任务
        env.execute();
    }
}
