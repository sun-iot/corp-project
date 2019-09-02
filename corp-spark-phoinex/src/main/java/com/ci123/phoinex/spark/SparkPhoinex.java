package com.ci123.phoinex.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.phoinex.spark
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/1 14:10
 */
public class SparkPhoinex {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName(Thread.currentThread().getName())
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        // 得到一个 session 会话
        SparkSession session = SparkSession
                .builder()
                .config(sparkConf)
                .config("spark.broadcast.compress", "false")
                .config("spark.shuffle.compress", "false")
                .config("spark.shuffle.spill.compress", "false")
                .config("spark.driver.host", "localhost")
                .getOrCreate() ;


        // JDBC连接属性
        Properties connProp = new Properties();
        connProp.put("driver", "org.apache.phoenix.jdbc.PhoenixDriver");
        connProp.put("user", "");
        connProp.put("password", "");
        connProp.put("fetchsize", 1000);

        Dataset<Row> load = session
                .read()
                .format("org.apache.phoenix.spark")
                .option("table", "us_population")
                .option("zkUrl" , "jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181")
                .load();
        load.registerTempTable("city_info");
//        JavaRDD<Row> map = session.read()
//                .jdbc("jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181", "sql语句", connProp)
//                .filter("true")
//                .javaRDD()
//                .mapToPair(r -> {
//                    return new Tuple2<>(r.getString(0), r.getString(1));
//                })
//                .distinct()
//                .groupByKey()
//                .map(r -> {
//                    StringBuffer sb = new StringBuffer();
//                    r._2().forEach(v -> sb.append(v));
//                    return RowFactory.create(r._1(), sb.toString());
//                });
//
//
//        for (Row row : map.collect()) {
//            System.out.println(row.get(1));
//        }
        //Dataset<Row> df = load.toDF().as();
        Dataset<Row> select_city_from_city_info = session.sql("select CITY from city_info");
        select_city_from_city_info.show();
       // load.show();

        session.stop();


    }
}