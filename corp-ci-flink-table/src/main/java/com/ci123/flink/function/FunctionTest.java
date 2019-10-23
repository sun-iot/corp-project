package com.ci123.flink.function;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.flink.function
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/24 11:49
 */
public class FunctionTest {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(env);


    }
}
