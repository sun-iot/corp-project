package com.ci123.hbase;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.catalog.InMemoryExternalCatalog;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.hbase
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/17 14:12
 */
public class HBaseByJDBC{
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
        DataSet<Emp> empDataSet = sourceHBase.map(new MapFunction<Row, Emp>() {
            @Override
            public Emp map(Row value) throws Exception {
                List<Emp> list = new ArrayList<Emp>();
                String strEmp = value.toString();
                String[] split = strEmp.split(",");
                Emp emp =  new Emp(split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7]) ;
                return emp ;
            }
        });
        // 获取表操作环境对象
        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(env);
        InMemoryExternalCatalog catalog = new InMemoryExternalCatalog("catalog");
        tableEnvironment.registerExternalCatalog("catalog" , catalog);
        // dataSet转化为一个table
        //  "empid,name,age,sex,email,company,address,deptid"
        Table table = tableEnvironment.fromDataSet(empDataSet , "empid,name,age,sex,email,company,address,deptid");
        // 注册成一个表
        tableEnvironment.registerTable("emp" , table );
        Table nameSumDeptid = tableEnvironment.scan("emp").select("name,deptid,email");

       // Table nameSumDeptid = tableEnvironment.sqlQuery("select name , deptid , email from emp ");
        DataSet<Result> resultDataSet = tableEnvironment.toDataSet(nameSumDeptid, Result.class);
        MapOperator<Result, Tuple3<String, String, String>> map = resultDataSet.map(new MapFunction<Result, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(Result value) throws Exception {
                return new Tuple3<>(value.name, value.deptid, value.email);
            }
        });
        Iterator<Tuple3<String, String, String>> iterator = map.collect().iterator();
        while (iterator.hasNext()){
            Tuple3<String, String, String> next = iterator.next();
            System.out.println("name:" + next.f0 + "\t" +
                    "deptid:" + next.f1 + "\t" +
                    "email:" + next.f2);
        }


    }


    public static class Emp{
        public String empid ;
        public String name ;
        public String age ;
        public String sex ;
        public String email ;
        public String company ;
        public String address ;
        public String deptid ;

        public Emp() {
            super();
        }

        public Emp(String empid, String name, String age, String sex, String email, String company, String address, String deptid) {
            this.empid = empid;
            this.name = name;
            this.age = age;
            this.sex = sex;
            this.email = email;
            this.company = company;
            this.address = address;
            this.deptid = deptid;
        }
    }

    public static class Result{
        public String name ;
        public String deptid ;
        public String email ;


        public Result() {
        }
    }
}





























