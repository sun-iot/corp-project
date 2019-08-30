package com.corp.flink.hbase;

import com.corp.flink.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.corp.flink.hbase
 * Version: 1.0
 * <p> 自定义的Sink,用以获取HBase中的数据
 * Created by SunYang on 2019/8/28 11:20
 */
public class HBaseSourceStream extends RichSourceFunction<Tuple2<String , String>> {
    // 数据库的表 table
    private String table = null ;
    private String quorum = "" ;
    private String port = "" ;
    public HBaseSourceStream(String table , String quorum , String port) {
        this.table = table ;
        this.quorum = quorum ;
        this.port = port ;
    }

    public HBaseSourceStream() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        HBaseUtil.setConf(quorum , port);
    }


    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        ResultScanner scan = HBaseUtil.scan(table);
        Iterator<Result> iterator = scan.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            String rowKey = Bytes.toString(result.getRow());
            StringBuffer sb = new StringBuffer();

            for (Cell cell : result.listCells()) {
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                sb.append(value).append(" ");
            }
            StringBuffer replace = sb.replace(sb.length() - 1, sb.length(), "");
            sourceContext.collect(new Tuple2<String , String>(rowKey , replace.toString()));
        }
    }

    @Override
    public void cancel() {
        HBaseUtil.close();
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setQuorum(String quorum) {
        this.quorum = quorum;
    }

    public void setPort(String port) {
        this.port = port;
    }
}
