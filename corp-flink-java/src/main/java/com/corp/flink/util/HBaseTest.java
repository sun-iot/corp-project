package com.corp.flink.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.corp.flink.util
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/8/28 14:30
 */
public class HBaseTest {
    @Test
    public void getHBase(){
        // 添加配置
        HBaseUtil.setConf("hadoop101,hadoop102,hadoop103" , "2181");
        // 得到连接
        ResultScanner scan = HBaseUtil.scan("student");
        Iterator<Result> iterator = scan.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            String rowKey = Bytes.toString(result.getRow());
            StringBuffer sb = new StringBuffer();

            for (Cell cell : result.listCells()) {
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                sb.append(value).append(",");
            }
            System.out.println(sb.toString());
        }
    }
    @Test
    public void addFamily(){
        // 添加配置
        HBaseUtil.setConf("hadoop101,hadoop102,hadoop103" , "2181");
       // HBaseUtil.addFaimily("student" , "grade");
        List list =  new ArrayList<String>();
        list.add("1001");
        list.add("1002") ;
        HBaseUtil.addMoreRecord("student" , "grade" , "Math" , list, "85");
    }
}
