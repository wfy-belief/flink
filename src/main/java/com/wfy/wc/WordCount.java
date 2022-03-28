package com.wfy.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


// 批处理word count
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据
        String inputPath = "src/main/resources/hello.txt";
        // 多态 指向父类
        DataSet<String> dataSource = env.readTextFile(inputPath);

        //对数据集进行操作，按照空格分词进行展开，转换为（word，1）进行统计
        DataSet<Tuple2<String, Integer>> result = dataSource.flatMap(new myFlatMapper())
                .groupBy(0) // 按照第一个位置的word分组
                .sum(1);// 将第二个位置上的数据求和
        result.print();
    }

    private static class myFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            // 按照空格分词
            String[] words = s.split(" ");
            // 遍历words 输出
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
