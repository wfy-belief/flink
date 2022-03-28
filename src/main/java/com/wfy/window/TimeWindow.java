package com.wfy.window;

import com.wfy.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TimeWindow {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/sensor.txt");


        // 使用lambda表达式
        DataStream<SensorReading> dataStream = streamSource.map(line -> {
            String[] fields = line.split(" ");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 窗口测试 windowAll() 不建议使用
        dataStream.keyBy(SensorReading::getId);
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
//                .timeWindow(Time.seconds(15));
//        .window(TumblingProcessingTimeWindows.of())
        env.execute();
    }
}
