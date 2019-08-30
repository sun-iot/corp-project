package com.corp.spark.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.corp.spark.util
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/8/29 14:30
 */
public class HBaseUtil {
    public static Configuration conf = null;
    public static Connection connection = null;
    public static Admin admin = null;

    /**
     * @desc 取得连接
     */
    public static void setConf(String quorum, String port) {

        System.setProperty("hadoop.home.dir", "G:\\development\\hadoop-2.7.2");
        conf = HBaseConfiguration.create();
        // zookeeper地址
        conf.set("hbase.zookeeper.quorum", quorum);
        conf.set("hbase.zookeeper.property.clientPort", port);

    }

    /**
     *
     * @param family
     * @param qualifiers
     * @return
     */
    public static Scan getScan(String family , String ...qualifiers){
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family)) ;
        for (String qualifier : qualifiers) {
            scan.addColumn(Bytes.toBytes(family) , Bytes.toBytes(qualifier));
        }
        return scan ;
    }
    public static Scan getScan(Scan scan , String family , String ...qualifiers){
        scan.addFamily(Bytes.toBytes(family)) ;
        for (String qualifier : qualifiers) {
            scan.addColumn(Bytes.toBytes(family) , Bytes.toBytes(qualifier));
        }
        return scan ;
    }


//    public static Scan getScan(String family , String[] qualifier){
//        Scan scan = new Scan();
//        scan.addColumn(Bytes.toBytes(String.valueOf(family)) , Bytes.toBytes(String.valueOf(qualifier))) ;
//        return scan ;
//    }

    /**
     *
     * @param family
     * @return
     */
    public static Scan getScan(String family){
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family)) ;
        return scan ;
    }

    /**
     *
     * @param scan
     * @param tableName
     * @return
     * @throws IOException
     */
    public static Configuration getScanConf(Scan scan , String tableName ) {
        conf.set(TableInputFormat.INPUT_TABLE , tableName);
        try {
            conf.set(TableInputFormat.SCAN , Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conf ;
    }

}