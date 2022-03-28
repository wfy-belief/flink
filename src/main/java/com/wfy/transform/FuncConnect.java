package com.wfy.transform;

import com.wfy.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class FuncConnect {
    public static void main(String[] args) throws Exception {
        // 获取 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/sensor.txt");


        // 使用lambda表达式
        DataStream<SensorReading> dataStream = streamSource.map(line -> {
            String[] fields = line.split(" ");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 1.分流
        SingleOutputStreamOperator<SensorReading> process = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading value, ProcessFunction<SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
//                https://blog.csdn.net/qq_42223694/article/details/118856218
                if (value.getTemperature() >= 27) {
                    out.collect(value);
                } else {
                    ctx.output(new OutputTag<SensorReading>("27") {
                    }, value);
                }
            }
        });

        // 选择
//        关于为什么会出现这个问题，我们知道在Java中泛型的实现方式是基于Code sharing机制的，
//        也就是对同一个原始类型下的泛型类型只生成同一份目标代码，即字节码，基于此机制JVM会将泛型的类型进行擦除，
//        这与cpp中的泛型实现有本质上的不同，因此也被称为假泛型
//        通过{}重写该类，显式指定该对象的泛型类型即可
        // https://blog.csdn.net/weixin_39598069/article/details/114714001
        // https://blog.csdn.net/qq_39598180/article/details/114491282
        DataStream<SensorReading> output = process.getSideOutput(new OutputTag<SensorReading>("27") {
        });
//        output.print("27");
//        process.print("all");

        SingleOutputStreamOperator<Tuple2<String, Double>> warning = process.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        // 合流 connect
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = warning.connect(output);

        DataStream<Double> result = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Double>() {
            @Override
            public Double map1(Tuple2<String, Double> value) throws Exception {
                return value.f1;
            }

            @Override
            public Double map2(SensorReading value) throws Exception {
                return value.getTemperature();
            }
        });

        result.print();

        env.execute();
    }
}
