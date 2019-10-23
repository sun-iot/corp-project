package com.ci123.flink.function;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.flink.function
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/10/11 18:05
 */
public class LastTimestamp extends AggregateFunction<Long, LastTimestamp.ACC> {


    @Override
    public ACC createAccumulator() {
        return new ACC();
    }

    @Override
    public Long getValue(ACC acc) {
        return acc.save;
    }

    public void accumulate(ACC acc, Long o) {
        acc.save = o;
    }

    public void resetAccumulator(ACC acc) {
        acc.save = null;
    }
    public void merge(ACC acc , Iterable<ACC> accs){
        Iterator<ACC> iterator = accs.iterator();
        while (iterator.hasNext()) {
            ACC next = iterator.next();
            acc.save = next.save ;
        }
    }

    public TypeInformation getResultType(){
        return BasicTypeInfo.LONG_TYPE_INFO ;
    }

    public static class ACC{
        Long save= 0L;
    }

}
