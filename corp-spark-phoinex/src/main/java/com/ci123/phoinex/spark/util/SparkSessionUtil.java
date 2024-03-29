package com.ci123.phoinex.spark.util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.phoinex.spark.util
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/2 9:37
 */
public class SparkSessionUtil {
    private static SparkSession session = null ;
    private static SparkConf sparkConf = null ;
    /**
     * 设置配置文件
     * @param name
     * @return
     */
    static {
        sparkConf = new  SparkConf().setMaster("local[*]")
                .setAppName("Spark")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("es.index.auto.create" , "true")
                .set("es.node","hadoop101")
                .set("es.port" , "9200")
                .set("es.wan.only" , "true");
    }
    /**
     * 得到 一个 session
     * @return
     */
    public static SparkSession getSession(String name){
        synchronized (SparkSessionUtil.class){
            if (session == null){
                session =  SparkSession.builder()
                        .master(name)
                        .config(sparkConf)
                        .config("spark.broadcast.compress", "false")
                        .config("spark.shuffle.compress", "false")
                        .config("spark.shuffle.spill.compress", "false")
                        .config("spark.driver.host", "localhost")
                        .getOrCreate();
            }
        }
        return session ;
    }
}
