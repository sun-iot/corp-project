package com.copr.flink.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CustomerFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String,Integer>> out) throws Exception {
        for (String word : s.split(" ")){
            out.collect(new Tuple2<String , Integer>(word , 1 ));
        }
    }
}
