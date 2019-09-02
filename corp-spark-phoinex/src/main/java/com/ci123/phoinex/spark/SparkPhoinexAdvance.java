package com.ci123.phoinex.spark;

import com.ci123.phoinex.spark.util.ConfigurationUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Properties;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.phoinex.spark
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/2 8:45
 */
public class SparkPhoinexAdvance {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]")
                .setAppName(Thread.currentThread().getName())
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .config("spark.broadcast.compress", "false")
                .config("spark.shuffle.compress", "false")
                .config("spark.shuffle.spill.compress", "false")
                .config("spark.driver.host", "localhost")
                .getOrCreate();

        Properties properties = new Properties();
        String phoinexUrl = ConfigurationUtil.getSparkPhoinexProperties("phoinex-url");

        properties.setProperty( "driver" , ConfigurationUtil.getSparkPhoinexProperties("phoinex-driver"));
        properties.setProperty("user" , ConfigurationUtil.getSparkPhoinexProperties("phoinex-user"));
        properties.setProperty("password" , ConfigurationUtil.getSparkPhoinexProperties("phoinex-password"));
        properties.setProperty("fetchsize" , ConfigurationUtil.getSparkPhoinexProperties("phoinex-fetchsize"));

        JavaPairRDD<String, Iterable<Integer>> rdd = session.read()
                .jdbc(phoinexUrl, "select CITY from us_population", properties)
                .javaRDD()
                .distinct()
                .mapToPair(r -> new Tuple2<>(r.getString(0), 1))
                .groupByKey();
        System.out.println(rdd.count());

        session.stop();
    }
}
