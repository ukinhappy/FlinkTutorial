package com.ukinhappy.apitest.source;

import com.ukinhappy.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class Collection {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1632407894L, 18.9),
                new SensorReading("sensor_2", 1632407992L, 19.9),
                new SensorReading("sensor_3", 1632407997L, 20.9)));

        dataStream.print();
        env.execute();
    }
}
