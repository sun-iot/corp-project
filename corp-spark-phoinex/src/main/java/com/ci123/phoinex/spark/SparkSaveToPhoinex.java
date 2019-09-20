package com.ci123.phoinex.spark;

import com.ci123.phoinex.spark.util.SparkSessionUtil;
import org.apache.spark.sql.SparkSession;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.phoinex.spark
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/9 16:02
 */
public class SparkSaveToPhoinex {
    public static void main(String[] args) {
        SparkSession session = SparkSessionUtil.getSession(Thread.currentThread().getName());

    }
}
