package com.wfy.produceStudy;

import com.wfy.beans.Job;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Func7 {
    /*
    不同经验下的平均薪资
     */
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/clear_data.txt");
        // 转换为job实体类
        SingleOutputStreamOperator<Job> Jobs = streamSource.map(new MapToJob());

        KeyedStream<Tuple3<String, Double, Integer>, String> keyBy = Jobs.map(new MapFunction<Job, Tuple3<String, Double, Integer>>() {
            @Override
            public Tuple3<String, Double, Integer> map(Job value) throws Exception {
                return new Tuple3<>(value.getEducation(), value.getSalary(), 1);
            }
        }).keyBy(item -> item.f0);

        SingleOutputStreamOperator<Tuple3<String, Double, Integer>> reduce = keyBy.reduce(new ReduceFunction<Tuple3<String, Double, Integer>>() {
            @Override
            public Tuple3<String, Double, Integer> reduce(Tuple3<String, Double, Integer> value1, Tuple3<String, Double, Integer> value2) throws Exception {
                return new Tuple3<>(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Double>> result = reduce.map(new MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(Tuple3<String, Double, Integer> value) throws Exception {
                return new Tuple2<>(value.f0, value.f1 / value.f2);
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
