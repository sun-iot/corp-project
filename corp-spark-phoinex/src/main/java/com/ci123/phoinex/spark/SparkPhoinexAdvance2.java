package com.ci123.phoinex.spark;

import com.ci123.phoinex.spark.util.ConfigurationUtil;
import com.ci123.phoinex.spark.util.SparkSessionUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.phoinex.spark
 * Version: 1.0
 * <p> 使用Spark通过Phoinex连接到HBase的进阶版，不查询整张表，只对表中的某些字段做查询
 * Created by SunYang on 2019/9/2 9:35
 */
public class SparkPhoinexAdvance2 {
    public static void main(String[] args) {
        SparkSession session = SparkSessionUtil.getSession(Thread.currentThread().getName());
        // select CITY from us_population
        Dataset empSet = session.read()
                .format(ConfigurationUtil.getSparkPhoinexProperties("phoinex-format"))
                .option("table", "emp")
                .option("zkUrl", ConfigurationUtil.getSparkPhoinexProperties("phoinex-zkurl"))
                .load();

        // empSet.show();
        empSet.registerTempTable("emp");

        Dataset deptSet = session.read()
                .format(ConfigurationUtil.getSparkPhoinexProperties("phoinex-format"))
                .option("table", "dept")
                .option("zkUrl", ConfigurationUtil.getSparkPhoinexProperties("phoinex-zkurl"))
                .load();
        // deptSet.show();
        deptSet.registerTempTable("dept");

        String sql = "select emp.name , emp.email , emp.company , emp.address , dept.dept_name , dept.dept_addr from emp left join dept on emp.deptid=dept.deptid";

        Dataset<Row> deptEmp = session.sql(sql);
        deptEmp.select("name" , "email").show();


        session.stop();

        /**
         * 第二种方式
         */
//        SparkConf conf = new SparkConf().setMaster("local[*]")
//                .setAppName(Thread.currentThread().getName())
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        SQLContext sqlContext = SQLContext.getOrCreate(new SparkContext(conf));
//        Map map = new HashMap<>();
//        map.put("table" , "us_population");
//        map.put("zkUrl" ,ConfigurationUtil.getSparkPhoinexProperties("phoinex-zkurl")) ;
//        Dataset load = sqlContext.load(ConfigurationUtil.getSparkPhoinexProperties("phoinex-format"), map);
//        load.registerTempTable("city");
//        load.show();
//
//        Dataset df = load.toDF();
//        df.show();
    }
}
