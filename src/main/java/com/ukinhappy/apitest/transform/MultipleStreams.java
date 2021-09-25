package com.ukinhappy.apitest.transform;

import com.ukinhappy.apitest.beans.SensorReading;
import com.ukinhappy.apitest.source.Collection;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

public class MultipleStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("/Users/ukinhappy/code/javaproject/FlinkTutorial/src/main/resources/sensor.txt");

        //构造sensor
        SingleOutputStreamOperator<SensorReading> sensorStream = inputStream.map(line -> {
            String[] fields = line.split(",");

            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        //split
        SplitStream<SensorReading> splitStream = sensorStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return (value.getTemperature() > 20) ? Collections.singleton("hight") : Collections.singleton("low");
            }
        });

        DataStream<SensorReading> low = splitStream.select("low");
        DataStream<SensorReading> hight = splitStream.select("hight");

        low.print();
        hight.print();
        env.execute();
    }
}
