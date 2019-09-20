package com.corp.flink.hbase;

import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.corp.flink.hbase
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/8/28 14:47
 */
public class HBaseSetFlink {
    public static void main(String[] args) {
        // 批处理获取HBase数据信息
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 实现 InputFormat
        env.createInput(new HBaseSourceInputFormat());


    }
}
