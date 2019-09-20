package com.corp.flink.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.corp.flink.java
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/8/28 10:34
 */
public class WordCountSet {
    public static void main(String[] args) {
        // 现拿到环境，请注意，这里的环境是  ExecutionEnvironment .
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 批处理 ， 使用DataSet
        DataSet<String> sourceSet = env.fromElements(
                "I am icon-man ", "If equal affection cannot be",
                " let the more loving be me",
                "Life is a load of running it is necessary to constantly in each choose a fork in the road"
        );

        DataSet<Tuple2<String, Integer>> sorceWordCount = sourceSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] contents = s.split(" ");
                for (String content : contents) {
                    if (content.length() >= 1) {
                        collector.collect(new Tuple2<String , Integer>(content, 1));
                    }
                }
            }
        }).groupBy(0).sum(1);
        try {
            sorceWordCount.print();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
