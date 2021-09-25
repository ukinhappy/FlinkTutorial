package com.ukinhappy.apitest.transform;

import com.ukinhappy.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.crypto.Data;

public class RichFunc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(4);

        DataStream<String> dataStream = env.readTextFile("/Users/ukinhappy/code/javaproject/FlinkTutorial/src/main/resources/sensor.txt");

        //构造Sensor
        SingleOutputStreamOperator<SensorReading> sensorStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });

        DataStream<Tuple2<String, Integer>> resultStream = sensorStream.map(new MyRichMapFunction());
        resultStream.print();

        env.execute();
    }

    public static class MyRichMapFunction extends RichMapFunction<SensorReading, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(SensorReading value) throws Exception {
            return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask());
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化工作，链接数据库等
            System.out.println("open");
            super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            //关闭资源等
            System.out.println("close");

            super.close();
        }
    }

}
