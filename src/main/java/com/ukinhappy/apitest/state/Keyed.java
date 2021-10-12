package com.ukinhappy.apitest.state;

import com.ukinhappy.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Keyed {
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

        SingleOutputStreamOperator<Integer> resultStream = sensorStream.keyBy(SensorReading::getId).map(new RichMapFunction<SensorReading, Integer>() {

            private ValueState<Integer> valueState;
            private ListState<String> listState;
            private MapState<String, Double> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value", Integer.class));
                listState = getRuntimeContext().getListState(new ListStateDescriptor<String>("list", String.class));
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("map", String.class, Double.class));
            }

            @Override
            public Integer map(SensorReading value) throws Exception {

                // value
                Integer count = valueState.value();
                if (count == null) {
                    count = 0;
                }
                count++;
                valueState.update(count);

                //list
                for (String str : listState.get()) {
                    System.out.println(str);
                }
                listState.add("list" + count.toString());
                //mapState
                mapState.put(value.getId(), value.getTemperature());
                System.out.println(mapState.get(value.getId()));
                return count;
            }
        });
        resultStream.print();

        env.execute();
    }
}
