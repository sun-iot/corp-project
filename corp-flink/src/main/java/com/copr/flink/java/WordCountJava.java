package com.copr.flink.java;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class WordCountJava {
    public static void main(String[] args) throws Exception {
        // 得到环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //
        SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = env.socketTextStream("hadoop101", 9999)
                .flatMap(new CustomerFlatMap()).keyBy(0)
                .timeWindow(Time.seconds(10)).sum(1);
        // 将得到的数据流打印输出
        dataStream.print();
        // 将得到的数据流输出到Kafka上
        dataStream.map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> tuple) throws Exception {
                return tuple.toString();
            }
        })
       .addSink(new FlinkKafkaProducer011("hadoop101:9092,hadoop102:9092,hadoop103:9092" , "flink_topic" , new SimpleStringSchema()));

        env.execute("WordCountJava");

    }
}
