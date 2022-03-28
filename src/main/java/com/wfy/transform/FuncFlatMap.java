package com.wfy.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FuncFlatMap {
    public static void main(String[] args) throws Exception {
        // 获取 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/sensor.txt");

        // flat map 按照空格切分字段
        SingleOutputStreamOperator<String> flatMap = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] strings = s.split(" ");
                for (String string : strings) {
                    collector.collect(string);
                }
            }
        });

        flatMap.print();

        env.execute();
    }
}

