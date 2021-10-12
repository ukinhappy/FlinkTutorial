package com.ukinhappy.apitest.window;

import com.ukinhappy.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);
        //并行调度 多个任务取最小watermark
        env.setParallelism(4);
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);

        //构造Sensor
        DataStream<SensorReading> sensorStream = inputDataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        })
                // 2s的延时 2s后再触发统计计算
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        System.out.println(element.getTimestamp() * 1000L);
                        return element.getTimestamp() * 1000L;
                    }
                });

        OutputTag<SensorReading> lateOut = new OutputTag<>("late");
        // min
        SingleOutputStreamOperator<SensorReading> resultStream = sensorStream.
                keyBy("id")
                // .window(TumblingEventTimeWindows.of(Time.seconds(10),Time.seconds(1))) //偏移
                .timeWindow(Time.seconds(10))
                .allowedLateness(Time.minutes(1))//延迟1分钟关闭
                .sideOutputLateData(lateOut)//关闭之后再来数据写入此处
                .maxBy("temperature");
        resultStream.print("maxTemp");
        resultStream.getSideOutput(lateOut).print("late");
        env.execute();
    }
}
