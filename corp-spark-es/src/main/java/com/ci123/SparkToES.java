package com.ci123;

import com.ci123.util.SparkSessionUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/9 17:15
 */
public class SparkToES {
    public static void main(String[] args) {
        SparkSession session = SparkSessionUtil.getSession(Thread.currentThread().getName());
        JavaSparkContext jsc = SparkSessionUtil.getJavaSparkContext();
        // select CITY from us_population
        Dataset empSet = session.read()
                .format("org.apache.phoenix.spark")
                .option("table", "emp")
                .option("zkUrl", "jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181")
                .load();

        // empSet.show();
        empSet.registerTempTable("emp");

        Dataset deptSet = session.read()
                .format("org.apache.phoenix.spark")
                .option("table", "dept")
                .option("zkUrl", "jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181")
                .load();
        // deptSet.show();
        deptSet.registerTempTable("dept");
        String sql = "select emp.name , emp.email , emp.company , emp.address , dept.dept_name , dept.dept_addr from emp left join dept on emp.deptid=dept.deptid";
        JavaRDD<Row> map = session.sql(sql).javaRDD();
//        jsc.parallelize(ImmutableList.of(map)) ;
        Iterator<Row> iterator = map.collect().iterator();
        while (iterator.hasNext()) {
            Row next = iterator.next();
            System.out.println(next.toString());
        }

        // JavaEsSpark.saveToEs(rowJavaRDD , "spark/docs");

        // 开始读取 ES 数据
//        JavaSparkContext contextES = new JavaSparkContext(session.sparkContext());
//        esRDD(contextES, "index_ik_test/fulltext", "?q=中国").values();

        session.stop();
    }
}
