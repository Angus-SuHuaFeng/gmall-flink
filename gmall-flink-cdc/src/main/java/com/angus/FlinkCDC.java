package com.angus;


import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author ：Angus
 * @date ：Created in 2022/4/23 20:10
 * @description：
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // TODO 1.1 开启checkPoint和状态后端
        env.setStateBackend(new FsStateBackend(new Path("hdfs://hadoop1:8020/gmall-flink/checkPoint")));
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // TODO 2.通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> mySqlSource = MySQLSource.<String>builder()
                .hostname("hadoop1")
                .port(3306)
                .databaseList("gmall")
                // TODO 必须 库名.表名
                .username("root")
                .password("000000")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(mySqlSource);

        dataStreamSource.addSink(MyKafkaUtil.getKafkaProducer("ods_base_db"));

        env.execute("flinkCDC: mysql to ods_base_db");
    }
}
