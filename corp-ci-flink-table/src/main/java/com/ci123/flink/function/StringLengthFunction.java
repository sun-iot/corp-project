package com.ci123.flink.function;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p> 自定义一个标量函数，用来统计字符串的长度
 * Project: corp-project
 * Package: com.ci123.flink.function
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/24 10:28
 */
public class StringLengthFunction extends ScalarFunction {

    // open 和 close 方法可以不写，如果写 ， 需要导入 import org.apache.flink.table.functions.FunctionContext;
    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
    }

    /**
     * 这个方法是自定义标量函数必须要有的方法
     * @param s
     * @return
     */
    public long eval(String s){
        return s == null ? 0 : s.length() ;
    }
}
