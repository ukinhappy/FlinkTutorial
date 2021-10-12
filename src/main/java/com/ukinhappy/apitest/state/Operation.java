package com.ukinhappy.apitest.state;

import com.ukinhappy.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Operation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);
        //并行调度 多个任务取最小watermark
        env.setParallelism(1);
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);

        //构造Sensor
        DataStream<SensorReading> sensorStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        SingleOutputStreamOperator<Integer> resultStream = sensorStream.map(new MapFunction<SensorReading, Integer>() {
            private Integer count = 0;

            @Override
            public Integer map(SensorReading value) throws Exception {
                count++;
                return count;
            }
        });
        resultStream.print();

        env.execute();
    }
}
