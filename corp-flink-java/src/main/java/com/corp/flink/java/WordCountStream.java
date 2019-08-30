package com.corp.flink.java;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountStream {
    public static void main(String[] args) {
        // 得到执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建一个socket源的输入
        DataStreamSource<String> source = env.socketTextStream("hadoop101", 8888);

        // 对我们的源开始做数据的分析和处理
        DataStream<Tuple2<String, Integer>> sourceWordCount = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] contents = s.split(" ");
                for (String content : contents) {
                    // 对数据所分析处理
                    collector.collect(new Tuple2<>(content, 1));
                }
            }
        });
        // 对数据所分组
        DataStream<Tuple2<String, Integer>> dataStream = sourceWordCount.keyBy(0).timeWindow(Time.seconds(5)).sum(1);

        dataStream.print();

        try {
            env.execute("WordCount");
        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}
