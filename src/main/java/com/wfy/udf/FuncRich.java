package com.wfy.udf;

import com.wfy.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 富函数
public class FuncRich {
    public static void main(String[] args) throws Exception {
        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取文件
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/sensor.txt");

        // 使用lambda表达式
        DataStream<SensorReading> dataStream = streamSource.map(line -> {
            String[] fields = line.split(" ");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<Tuple2<String, Integer>> result = dataStream.map(new MyMapper());

        result.print();
        env.execute();
    }

    private static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 打开状态或者建立连接
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close");
        }
    }
}
