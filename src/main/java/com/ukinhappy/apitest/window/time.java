package com.ukinhappy.apitest.window;

import com.ukinhappy.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;


public class time {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);

        //构造Sensor
        DataStream<SensorReading> sensorStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        // 1. 增量聚合函数

        // max
//        DataStream<SensorReading> resultStream = sensorStream.
//                keyBy("id")
//                .timeWindow(Time.seconds(5))
//                .max("temperature");
//        resultStream.print();

        //reduce
//
//        DataStream<SensorReading> resultStream = sensorStream.
//                keyBy("id")
//                .timeWindow(Time.seconds(5))
//                .reduce(new ReduceFunction<SensorReading>() {
//                    @Override
//                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
//                        return value1.getTemperature() > value2.getTemperature() ? value1 : value2;
//                    }
//                });
//        resultStream.print();

//
//        SingleOutputStreamOperator<Integer> resultStream = sensorStream.
//                keyBy("id")
//                .timeWindow(Time.seconds(5))
//                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
//                    @Override
//                    public Integer createAccumulator() {
//                        return 0;
//                    }
//
//                    @Override
//                    public Integer add(SensorReading value, Integer accumulator) {
//                        return accumulator + 1;
//                    }
//
//                    @Override
//                    public Integer getResult(Integer accumulator) {
//                        return accumulator;
//                    }
//
//                    @Override
//                    public Integer merge(Integer a, Integer b) {
//                        return null;
//                    }
//                });
//
//
//        resultStream.print();

        //2. 全窗口函数
        SingleOutputStreamOperator<Double> resultStream = sensorStream.keyBy("id").timeWindow(Time.seconds(5))
                .apply(new WindowFunction<SensorReading, Double, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Double> out) throws Exception {
                        Double temperatureTotal = 0.0;
                        Iterator<SensorReading> iterator = input.iterator();
                        Integer count =0;
                        while (iterator.hasNext()) {
                            count++;
                            temperatureTotal += iterator.next().getTemperature();
                        }
                       // System.out.println(IteratorUtils.toList(iterator).size());
                       // out.collect(temperatureTotal / (IteratorUtils.toList(iterator).size()));
                        out.collect(temperatureTotal / count);

                    }
                });
        resultStream.print();
        env.execute();
    }
}
