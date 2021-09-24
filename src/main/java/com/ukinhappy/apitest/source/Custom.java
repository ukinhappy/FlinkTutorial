package com.ukinhappy.apitest.source;

import com.ukinhappy.apitest.beans.SensorReading;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SensorReading> dataStream = env.addSource(new MySensorSource());
        dataStream.print();
        env.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading> {

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            Random random = new Random();
            while (true) {
                ctx.collect(new SensorReading("sendid", System.currentTimeMillis(), random.nextDouble()));
                Thread.sleep(2000L);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
