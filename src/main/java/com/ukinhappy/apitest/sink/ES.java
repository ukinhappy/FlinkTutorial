package com.ukinhappy.apitest.sink;

import com.ukinhappy.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.bytes.BytesReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.readTextFile("/Users/ukinhappy/code/javaproject/FlinkTutorial/src/main/resources/sensor.txt");

        //构造Sensor
        DataStream<SensorReading> sensorStream = dataStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.valueOf(fields[1]), Double.valueOf(fields[2]));
        });
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));
        sensorStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new MyEsSinkFunction()).build());

        env.execute();
    }

    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading> {

        @Override
        public void process(SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {
            HashMap<String, String> data = new HashMap<>();
            data.put("id", element.getId());
            data.put("ts", element.getTimestamp().toString());
            data.put("temp", element.getTemperature().toString());
            IndexRequest sensorSource = Requests.indexRequest().index("sensor").id(element.getId()).source(data);
            indexer.add(sensorSource);
        }

        @Override
        public void open() throws Exception {

        }

        @Override
        public void close() throws Exception {

        }


    }
}
