package com.wfy.source;

import com.wfy.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class FromCollection {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 一个并行度有序，按照大数据多分区理解
//        env.setParallelism(1);

        // 从集合中读取数据
        DataStreamSource<SensorReading> streamSource = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 12374625L, 23.3),
                new SensorReading("sensor_6", 12384385L, 25.9),
                new SensorReading("sensor_7", 14374625L, 13.0),
                new SensorReading("sensor_10", 18374625L, 32.8)
        ));

        // 从元素中得到流
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 3, 4, 6, 7);

        //打印输出
        streamSource.print("data");
        integerDataStreamSource.print("int");
        // 如果按照下面写法而且 输入和输出 直接没有任何 转换的过程 那么就是输入输出合并到一起进行进行，也会有序输出。
//        integerDataStreamSource.print("int").setParallelism(1);

        // execution
        env.execute("collection test");
    }
}
