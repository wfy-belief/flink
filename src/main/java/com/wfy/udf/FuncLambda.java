package com.wfy.udf;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// 匿名函数
public class FuncLambda {
    public static void main(String[] args) {
        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取文件
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/sensor.txt");

        DataStream<String> filter = streamSource.filter(value -> value.startsWith("start"));
    }

}
