package com.ukinhappy.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 *
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 读取数据流
        String path = "/Users/ukinhappy/code/javaproject/FlinkTutorial/src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(path);
        /// 对数据进行处理，按空格分词展开，转换成（word,1） 二元组进行统计
        DataSet<Tuple2<String, Integer>> resultSet = inputDataSet.flatMap(new MyflatMapper()).
                groupBy(0). //按照第一个位置的word分组
                sum(1);

        resultSet.print();
    }

    // 自定义类
    public static class MyflatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
