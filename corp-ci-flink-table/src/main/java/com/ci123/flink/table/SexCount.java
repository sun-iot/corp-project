package com.ci123.flink.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.flink.table
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/23 10:49
 */
public class SexCount {
    // 根据性别分组，统计男女的数量
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Row> sourceHBase = env.createInput(new JDBCInputFormat().buildJDBCInputFormat()
                .setDrivername("org.apache.phoenix.jdbc.PhoenixDriver")
                .setDBUrl("jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181")
                .setFetchSize(1024)
                .setQuery("select * from emp ")
                .setRowTypeInfo(new RowTypeInfo(new TypeInformation[] {
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                }))
                .finish());

        // 开始转化成EMP
        MapOperator<Row, HBaseTable.Emp> map = sourceHBase.map(new MapFunction<Row, HBaseTable.Emp>() {
            @Override
            public HBaseTable.Emp map(Row row) throws Exception {
                String s = row.toString();
                String[] split = s.split(",");
                return new HBaseTable.Emp(split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7]);
            }
        });

//        map.print();
        // 开始把EMP映射成一张表
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(env);
        Table tableEmp = tableEnvironment.fromDataSet(map, "empid,name,age,sex,email,company,address,deptid");
        // 注册表
        tableEnvironment.registerTable("Emp" , tableEmp);
//        Table table = tableEnvironment.sqlQuery("select sex.count from Emp");
        Table select = tableEmp.groupBy("sex").select("sex,Count(sex)");
        DataSet<Row> countSex = tableEnvironment.toDataSet(select, Row.class);
        countSex.print();
    }
}