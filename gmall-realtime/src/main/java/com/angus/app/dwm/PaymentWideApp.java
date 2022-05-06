package com.angus.app.dwm;

import com.angus.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author ：Angus
 * @date ：Created in 2022/5/6 22:01
 * @description：
 */
public class PaymentWideApp {
    public static void main(String[] args) {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO 1.1 开启checkPoint和状态后端
//        env.setStateBackend(new FsStateBackend(new Path("hdfs://hadoop1:8020/gmall-flink/checkPoint")));
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // TODO 2.读取Kafka主题的数据
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        DataStreamSource<String> paymentKafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(paymentInfoSourceTopic, groupId));

        
    }
}
