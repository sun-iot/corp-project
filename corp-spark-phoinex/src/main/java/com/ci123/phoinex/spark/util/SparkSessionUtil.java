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

    /**
     * 设置配置文件
     * @param name
     * @return
     */
    private static SparkConf setConf(String name){
        return new SparkConf().setMaster("local[*]")
                .setAppName(Thread.currentThread().getName())
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    }
    /**
     * 得到 一个 session
     * @return
     */
    public static SparkSession getSession(String name){
        synchronized (SparkSessionUtil.class){
            if (session == null){
                session =  SparkSession.builder()
                        .config(setConf(name))
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
