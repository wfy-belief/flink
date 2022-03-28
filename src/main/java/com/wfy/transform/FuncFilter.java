package com.wfy.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FuncFilter {
    public static void main(String[] args) throws Exception {
        // 获取 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/sensor.txt");

        // filter 只是删选数据 而不做数据的转换
        SingleOutputStreamOperator<String> filter = streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor_1");
            }
        });

        filter.print();

        env.execute();
    }
}

