package com.ci123;

import com.ci123.source.HBaseInputFormat;
import com.ci123.util.ConfigConstant;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/16 14:36
 */
public class HBaseSet {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<ResultScanner> student = execEnv.createInput(
                HBaseInputFormat.buildHBaseInputFormat()
                        .setQuorum(ConfigConstant.getVal("hbase.zookeeper.quorum"))
                        .setClientPort("2181")
                        .setTableName("student").openInput()
                        .finish()
        );
        // student.print();
       // System.out.println(student.collect().size());

    }
}
