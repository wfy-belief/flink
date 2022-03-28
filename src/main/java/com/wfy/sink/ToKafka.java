package com.wfy.sink;

import com.wfy.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class ToKafka {
    public static void main(String[] args) throws Exception {
        // 获取 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/sensor.txt");


        // 使用lambda表达式
        DataStream<String> dataStream = streamSource.map(line -> {
            String[] fields = line.split(" ");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });

        // 连接器，官网上面可以查看支持的连接器
        dataStream.addSink(
                new FlinkKafkaProducer011<String>(
                        "ubuntu:9092",
                        "test",
                        new SimpleStringSchema()));

        env.execute();

    }
}
