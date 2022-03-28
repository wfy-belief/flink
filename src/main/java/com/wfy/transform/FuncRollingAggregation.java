package com.wfy.transform;

import com.wfy.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FuncRollingAggregation {
    public static void main(String[] args) throws Exception {
        // 获取 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/sensor.txt");

        // 转换为sensorReading类型
//        DataStream<SensorReading> dataStream = streamSource.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] fields = s.split(" ");
//                return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
//            }
//        });

        // 使用lambda表达式
        DataStream<SensorReading> dataStream = streamSource.map(line -> {
            String[] fields = line.split(" ");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 分组
        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);
//        id.print();
//        KeyedStream<SensorReading, String> keyedStream = dataStream.keyBy(SensorReading::getId);// 方法引用

//        keyedStream.print("group");
        // 聚合 需要无参构造函数
        keyedStream.maxBy("temperature").print();


        env.execute();
    }
}
