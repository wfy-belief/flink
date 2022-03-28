package com.wfy.udf;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// 函数类
public class FuncClasses {
    public static void main(String[] args) {
        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取文件
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/sensor.txt");

        DataStream<String> filter = streamSource.filter(new MyFilter("start"));
    }

    private static class MyFilter implements FilterFunction<String> {
        private final String keyWord;

        // 构造方法传递
        MyFilter() {
            this.keyWord = "start";
        }

        MyFilter(String keyWord) {
            this.keyWord = keyWord;
        }

        @Override
        public boolean filter(String value) throws Exception {
            return value.startsWith(keyWord);
        }
    }
}
