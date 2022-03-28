package com.wfy.produceStudy;

import com.wfy.beans.Job;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Func1 {
    /*
    所有的平均薪资
     */
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/clear_data.txt");

        SingleOutputStreamOperator<Job> Jobs = streamSource.map(new MapToJob());
        SingleOutputStreamOperator<Tuple2<Double, Integer>> salaryStream = Jobs.map(value -> new Tuple2<>(value.getSalary(), 1));

        SingleOutputStreamOperator<Tuple2<Double, Integer>> reduce = salaryStream.keyBy(item -> 1).reduce(new ReduceFunction<Tuple2<Double, Integer>>() {
            @Override
            public Tuple2<Double, Integer> reduce(Tuple2<Double, Integer> value1, Tuple2<Double, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0 + value2.f0, value1.f1 + value2.f1);
            }
        });

        SingleOutputStreamOperator<Double> avg = reduce.map(item -> item.f0 / item.f1);
        avg.print();
//        streamSource.print();

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
