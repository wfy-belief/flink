package com.wfy.produceStudy;

import com.wfy.beans.Job;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Func5 {
    /*
    不同城市每个公司发布的岗位数量TOP10
     */
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.readTextFile("src/main/resources/clear_data.txt");
        // 转换为job实体类
        SingleOutputStreamOperator<Job> Jobs = streamSource.map(new MapToJob());



        // result.print();

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
