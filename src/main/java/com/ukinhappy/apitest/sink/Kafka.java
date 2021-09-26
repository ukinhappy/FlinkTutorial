package com.ukinhappy.apitest.sink;

import com.ukinhappy.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.Kafka011TableSink;

import java.util.Properties;

public class Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //建立消费者 消费ukinhappy_topic 数据
        String topic = "ukinhappy_topic";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>(topic, new SimpleStringSchema(), properties));

        //写入 sink_topic topic中
        dataStream.addSink(new FlinkKafkaProducer011<String>("localhost:9092", "sink_topic", new SimpleStringSchema()));

        env.execute();

    }
}
