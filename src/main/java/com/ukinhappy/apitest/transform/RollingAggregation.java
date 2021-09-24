package com.ukinhappy.apitest.transform;

import com.ukinhappy.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> dataStream = env.readTextFile("/Users/ukinhappy/code/javaproject/FlinkTutorial/src/main/resources/sensor.txt");

        DataStream<SensorReading> sensorStream = dataStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
            }
        });


        KeyedStream<SensorReading, Tuple> keyedStream = sensorStream.keyBy("id");

        //滚动聚合取最大值
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.maxBy("temperature");

        resultStream.print();
        env.execute();

    }
}
