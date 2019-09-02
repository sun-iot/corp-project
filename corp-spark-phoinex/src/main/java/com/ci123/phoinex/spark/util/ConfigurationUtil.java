package com.ci123.phoinex.spark.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.phoinex.spark.util
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/2 9:01
 */
public class ConfigurationUtil {

    private static ResourceBundle resourceBundle;
    static{
        resourceBundle = ResourceBundle.getBundle("condition");
    }

    /**
     * 从条件的json中获取条件数据
     * @param condition
     * @return
     */
    public static String getValueOfCondition(String condition){
        String conditionJson = resourceBundle.getString("condition.params.json");
        JSONObject jsonObject = JSON.parseObject(conditionJson);
        return jsonObject.get(condition).toString();
    }

    /**
     * 从 spark-phoinex.properties获取相关配置信息
     * @param key
     * @return
     */
    public static String getSparkPhoinexProperties(String key){
        InputStream sparkPhoinex = Thread.currentThread().getContextClassLoader().getResourceAsStream("spark-phoinex.properties");
        Properties properties = new Properties();
        try {
            properties.load(sparkPhoinex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties.getProperty(key) ;
    }
}
