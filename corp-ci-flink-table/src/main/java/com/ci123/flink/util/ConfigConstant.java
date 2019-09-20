package com.ci123.flink.util;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.flink.util
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/18 11:12
 */
public class ConfigConstant {
    private static Map<String, String> valueMap = new HashMap<String, String>();

    static {
        // 国际化
        ResourceBundle ct = ResourceBundle.getBundle("es");
        Enumeration<String> enums = ct.getKeys();
        while ( enums.hasMoreElements() ) {
            String key = enums.nextElement();
            String value = ct.getString(key);
            valueMap.put(key, value);
        }

    }

    public static String getVal( String key ) {
        return valueMap.get(key);
    }

}
