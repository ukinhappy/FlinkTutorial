package com.ukinhappy.apitest.transform;

import com.ukinhappy.apitest.beans.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

public class Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> dataStream = env.readTextFile("/Users/ukinhappy/code/javaproject/FlinkTutorial/src/main/resources/sensor.txt");


        //new sensor stream
        DataStream<SensorReading> sensorStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        DataStream<Tuple2<String, Integer>> tupleStream = env.fromCollection(Arrays.asList(
                new Tuple2<String, Integer>("ukinhappy", 1111),
                new Tuple2<String, Integer>("Ukinhappy", 2222)));


        //connect
        ConnectedStreams<Tuple2<String, Integer>, SensorReading> connectedStreams = tupleStream.connect(sensorStream);

        DataStream<String> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Integer>, SensorReading, String>() {
            @Override
            public String map1(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }

            @Override
            public String map2(SensorReading value) throws Exception {
                return value.getId();
            }
        });

        resultStream.print();
        env.execute();
    }
}
