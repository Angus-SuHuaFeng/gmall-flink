package com.angus.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author ：Angus
 * @date ：Created in 2022/4/24 17:16
 * @description：
 */
public class MyKafkaUtil {
    private static String brokerList = "hadoop1:9092,hadoop2:9092,hadoop3:9092";
    private static String default_topic = "DWD_DEFAULT_TOPIC";
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
        return new FlinkKafkaProducer<String>(brokerList, topic, new SimpleStringSchema());
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String group){

        Properties prop = new Properties();
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(StandardCharsets.UTF_8), prop);
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        return new FlinkKafkaProducer<T>(default_topic, kafkaSerializationSchema, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
