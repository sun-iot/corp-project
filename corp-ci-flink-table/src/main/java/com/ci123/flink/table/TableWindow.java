package com.ci123.flink.table;

import com.ci123.flink.function.LastTimestamp;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.types.Row;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.flink.table
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/10/11 15:08
 */
public class TableWindow {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Row> sourceHBase = env.createInput(new JDBCInputFormat().buildJDBCInputFormat()
                .setDrivername("org.apache.phoenix.jdbc.PhoenixDriver")
                .setDBUrl("jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181")
                .setFetchSize(1024)
                .setQuery("select * from employee ")
                .setRowTypeInfo(new RowTypeInfo(new TypeInformation[] {
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                }))
                .finish());

        // 定义Table的环境变量
            BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        // 将dataSet转化为table
        Table tableEmp = tableEnv.fromDataSet(sourceHBase, "__time,empId,name,age,sex,email,company");
        // 注册tableEmp
        tableEnv.registerTable("Emp" , tableEmp);

        // 扫描整张表，查询字段
       // Table empTable = tableEnv.scan("Emp").as("__time,empId,name,age,sex,email,company");
        tableEnv.registerFunction("LastTimesatamp", new LastTimestamp());
        Table select = tableEmp.window(Tumble.over("1.minutes").on("__time").as("w"))
                .groupBy("age,sex,company,w")
                .select("LastTimesatamp(__time),age.avg,sex,company");


        // timestamp , dimensions... , count

//        select.printSchema();
        DataSet<Row> rowDataSet = tableEnv.toDataSet(select, Row.class);
        rowDataSet.print();
        System.out.println("count：" + rowDataSet.count());

        DataSet<Row> dataSet = tableEnv.toDataSet(select.distinct(), Row.class);
        dataSet.print();
        System.out.println("distinct count: " + dataSet.count());


    }
}


