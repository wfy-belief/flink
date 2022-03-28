package com.wfy.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class FromKafka {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从kafka读取数据 需要 connector kafka依赖
        // 不能直接从 kafka 调用
        // 使用 add source 调用
        // 三个参数 topic 主题 反序列化 properties
        Properties properties = new Properties();
        FlinkKafkaConsumer011<String> sensor = new FlinkKafkaConsumer011<>("sensor", new SimpleStringSchema(), properties);
        DataStreamSource<String> streamSource = env.addSource(sensor);
        streamSource.print();

        env.execute();
    }
}
