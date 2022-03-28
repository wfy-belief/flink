package com.wfy.produceStudy;

import com.wfy.beans.Job;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Func4 {
    /*
    不同城市不同经验下的平均薪资
     */
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/clear_data.txt");
        // 转换为job实体类
        SingleOutputStreamOperator<Job> Jobs = streamSource.map(new MapToJob());

        KeyedStream<Tuple4<String, String, Double, Integer>, Tuple2<String, String>> keyBy = Jobs.map(new MapFunction<Job, Tuple4<String, String, Double, Integer>>() {
            @Override
            public Tuple4<String, String, Double, Integer> map(Job value) throws Exception {
                return new Tuple4<>(value.getCity(), value.getExperience(), value.getSalary(), 1);
            }
        }).keyBy(new KeySelector<Tuple4<String, String, Double, Integer>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple4<String, String, Double, Integer> value) throws Exception {
                return new Tuple2<>(value.f0, value.f1);
            }
        });

        SingleOutputStreamOperator<Tuple4<String, String, Double, Integer>> reduce = keyBy.reduce(new ReduceFunction<Tuple4<String, String, Double, Integer>>() {
            @Override
            public Tuple4<String, String, Double, Integer> reduce(Tuple4<String, String, Double, Integer> value1, Tuple4<String, String, Double, Integer> value2) throws Exception {
                return new Tuple4<>(value1.f0, value1.f1, value1.f2 + value2.f2, value1.f3 + value2.f3);
            }
        });

        SingleOutputStreamOperator<Tuple3<String, String, Double>> result = reduce.map(new MapFunction<Tuple4<String, String, Double, Integer>, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(Tuple4<String, String, Double, Integer> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, value.f2 / value.f3);
            }
        });

        result.print();

        //执行环境
        env.execute();

    }

    private static class MapToJob implements MapFunction<String, Job> {
        @Override
        public Job map(String value) throws Exception {
            String[] split = value.split("\\|");
            return new Job(split[0], split[1], split[2], split[3], split[4], split[5]);
        }
    }
}
