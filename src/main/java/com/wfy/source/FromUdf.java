package com.wfy.source;

import com.wfy.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

public class FromUdf {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从自定义source读取数据
        DataStreamSource<SensorReading> streamSource = env.addSource(new MySensorSource());

        streamSource.print();

        env.execute();
    }

    private static class MySensorSource implements SourceFunction<SensorReading> {
        // 定义一个标记位 用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            // 定义一个随机数发生器
            Random random = new Random();

            // 设置10个传感器的初始温度
            HashMap<String, Double> sensorMap = new HashMap<>();

            for (int i = 0; i < 10; i++) {
                sensorMap.put("sensor_" + (i + 1), 20.0);
            }

            // 不停的生成数据
            while (running) {
                for (String key : sensorMap.keySet()) {
                    // 在当前基础上面加上随机波动
                    double newT = sensorMap.get(key) + random.nextGaussian() * 10;

                    // 更新键值对
                    sensorMap.put(key, newT);

                    // collect
                    sourceContext.collect(new SensorReading(key, System.currentTimeMillis(), newT));
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
