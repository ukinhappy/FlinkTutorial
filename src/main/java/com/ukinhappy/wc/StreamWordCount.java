package com.ukinhappy.wc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环形
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置最大并行数
        env.setParallelism(4);

        //1,从文件中读取数据
        //String path = "/Users/ukinhappy/code/javaproject/FlinkTutorial/src/main/resources/hello.txt";
        //DataStream<String> inputDataStream = env.readTextFile(path);


        //2. 从socket文本流中读取数据
        //DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);
        // 用parammeter tool工具从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        DataStream<String> inputDataStream = env.socketTextStream(parameterTool.get("host"), parameterTool.getInt("port"));


        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyflatMapper())
                .keyBy(0).sum(1);

        resultStream.print();

        //执行任务
        env.execute();
    }
}
