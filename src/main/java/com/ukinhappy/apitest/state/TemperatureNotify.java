package com.ukinhappy.apitest.state;

import com.ukinhappy.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple3;

public class TemperatureNotify {
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

        SingleOutputStreamOperator<Tuple3<Long, Double, Double>> resultStream = sensorStream.keyBy(SensorReading::getId).flatMap(new RichFlatMapFunction<SensorReading, Tuple3<Long, Double, Double>>() {

            private ValueState<Double> lastTemperature;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastTemperature = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last", Double.class));
            }

            @Override
            public void flatMap(SensorReading value, Collector<Tuple3<Long, Double, Double>> out) throws Exception {
                if (lastTemperature.value() != null) {
                    if (Math.abs(lastTemperature.value() - value.getTemperature()) > 10) {
                        out.collect(new Tuple3<>(value.getTimestamp(), lastTemperature.value(), value.getTemperature()));
                    }
                }

                lastTemperature.update(value.getTemperature());
            }

            @Override
            public void close() throws Exception {
                lastTemperature.clear();

            }
        });

        resultStream.print();
        env.execute();
    }
}
