package com.wfy.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamNc {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 用parameter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // socket
        DataStreamSource<String> streamSource = env.socketTextStream(host, port);

        // 基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = streamSource.flatMap(new myMapper())
//                .keyBy(0)
                .keyBy(tuple -> tuple.f0)
                .sum(1);
        result.print();

        // 执行任务
        env.execute();
    }

    private static class myMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            // 按照空格分词
            String[] words = s.split(" ");
            // 遍历所有word，包装成二元组输出
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
