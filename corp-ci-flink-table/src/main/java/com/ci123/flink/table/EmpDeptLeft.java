package com.ci123.flink.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
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
 * Created by SunYang on 2019/9/23 11:14
 */
public class EmpDeptLeft {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Row> sourceHBase = env.createInput(new JDBCInputFormat().buildJDBCInputFormat()
                .setDrivername("org.apache.phoenix.jdbc.PhoenixDriver")
                .setDBUrl("jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181")
                .setFetchSize(1024)
                .setQuery("select * from emp ")
                .setRowTypeInfo(new RowTypeInfo(new TypeInformation[]{
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
        DataSet<HBaseTable.Emp> empDataSet = sourceHBase.map(new MapFunction<Row, HBaseTable.Emp>() {
            @Override
            public HBaseTable.Emp map(Row value) throws Exception {
                String strEmp = value.toString();
                String[] split = strEmp.split(",");
                HBaseTable.Emp emp = new HBaseTable.Emp(split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7]);
                return emp;
            }
        });
        // 第二张数据源表
        DataSource<Row> sourceDeptHBase = env.createInput(new JDBCInputFormat().buildJDBCInputFormat()
                .setDrivername("org.apache.phoenix.jdbc.PhoenixDriver")
                .setDBUrl("jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181")
                .setFetchSize(1024)
                .setQuery("select * from dept ")
                .setRowTypeInfo(new RowTypeInfo(new TypeInformation[]{
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                }))
                .finish());

        DataSet<HBaseTable.Dept> deptDataSet = sourceDeptHBase.map(new MapFunction<Row, HBaseTable.Dept>() {
            @Override
            public HBaseTable.Dept map(Row value) throws Exception {
                String strEmp = value.toString();
                String[] split = strEmp.split(",");
                HBaseTable.Dept dept = new HBaseTable.Dept(split[0], split[1], split[2], split[3]);
                return dept;
            }
        });

        // 定义Table的环境变量
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        // 将dataSet转化为table
        Table tableEmp = tableEnv.fromDataSet(empDataSet, "empid,name,age,sex,email,company,address,deptid");
        Table tableDept = tableEnv.fromDataSet(deptDataSet, "deptId,deptName,deptManager,deptAddr");

        // 注册tableEmp
        tableEnv.registerTable("Emp", tableEmp);
        tableEnv.registerTable("Dept", tableDept);

        // 扫描整张表，查询字段
        Table enmpTable = tableEnv.scan("Emp").as("empid,name,age,sex,email,company,address,deptid");
        Table deptTable = tableEnv.scan("Dept").as("id,deptName,deptManager,deptAddr");
        // Table selectNameDeptidEmail = enmpTable.select("name,age,sex,email,company");


        Table selectLeftJoinEmpDept = enmpTable.leftOuterJoin(deptTable, "deptid == id").select("name,age,sex,email,company,deptName,deptManager");

//        Table selectLeftJoinEmpDept = enmpTable.join(deptTable).where("deptid=id ").select("name,age,sex,email,company,deptName,deptManager");
        selectLeftJoinEmpDept.printSchema();

        DataSet<Row> rowDataSet = tableEnv.toDataSet(selectLeftJoinEmpDept, Row.class);

        rowDataSet.print();
    }

    public static class Emp {
        public String empid;
        public String name;
        public String age;
        public String sex;
        public String email;
        public String company;
        public String address;
        public String deptid;

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


    public static class Dept {
        public String deptId;
        public String deptName;
        public String deptManager;
        public String deptAddr;

        public Dept() {
        }

        public Dept(String deptId, String deptName, String deptManager, String deptAddr) {
            this.deptId = deptId;
            this.deptName = deptName;
            this.deptManager = deptManager;
            this.deptAddr = deptAddr;
        }
    }
}
