package com.corp.flink.hbase;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.corp.flink.hbase
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/8/28 13:48
 */
public class HBaseStreamFlink {
    public static void main(String[] args) {
         ExecutionEnvironment envP = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 事件时间
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        HBaseSourceStream hbaseSource = new HBaseSourceStream();
        hbaseSource.setQuorum("hadoop101,hadoop102,hadoop103");
        hbaseSource.setPort("2181");
        hbaseSource.setTable("student");
        DataStreamSource<Tuple2<String, String>> sourceHBase = env.addSource(hbaseSource);

        DataStream<Tuple2<String, Integer>> sexCount = sourceHBase.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(Tuple2<String, String> stringStringTuple2, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] info = stringStringTuple2.f1.split(" ");
                System.out.println(info[2]);
                collector.collect(new Tuple2<>(info[2], 1));
            }
        }).keyBy(0).sum(1);
        sexCount.print();

//        sourceHBase.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String , Long>>() {
//            @Override
//            public void flatMap(Tuple2<String, String> stringStringTuple2, Collector<Tuple2<String, Long>> collector) throws Exception {
//                String[] info = stringStringTuple2.f1.split(" ");
//
//                collector.collect(new Tuple2<String , Long>(info[1] , LocalDateTime.now().toInstant(ZoneOffset.of("+8")).toEpochMilli()));
//            }
//        }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
//            // 最大的乱序时间
//            long maxOutOfOrderness  = 3500L ;
//            // 当前的最大时间戳
//            long currentMaxTimestamp = 0L ;
//            @Nullable
//            @Override
//            public Watermark getCurrentWatermark() {
//                return new Watermark(maxOutOfOrderness  - currentMaxTimestamp);
//            }
//
//            @Override
//            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
//                // 得到数据的时间戳
//                long timestamp = element.f1.longValue();
//                currentMaxTimestamp = Math.max(timestamp , currentMaxTimestamp);
//                return timestamp;
//            }
//        });


        // sourceHBase.print() ;

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
