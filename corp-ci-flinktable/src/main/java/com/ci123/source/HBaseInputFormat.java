package com.ci123.source;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.source
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/16 11:16
 */
public class HBaseInputFormat extends RichInputFormat<ResultScanner , InputSplit>  {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HBaseInputFormat.class);

    private org.apache.hadoop.conf.Configuration conf = null;
    private Connection connection = null;
    private Admin admin = null;
    private String tableName ;
    private String quorum ;
    private String clientPort ;
    private ResultScanner scanner ;

    @Override
    public void configure(Configuration parameters) {
        //do nothing here
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (minNumSplits < 1 ){
            throw new IllegalArgumentIOException("Number of input splits must be at least 1.");
        }
        minNumSplits = (this instanceof NonParallelInput ) ? 1 : minNumSplits ;
        GenericInputSplit[] splits = new GenericInputSplit[minNumSplits];
        for (int i = 0 ; i < splits.length ; i ++ ){
            splits[i] = new GenericInputSplit(i , minNumSplits) ;
        }
        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }
    // 打开与HBase的连接
    @Override
    public void openInputFormat()  {
        System.out.println("openInputFormat...");
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum" , quorum);
        conf.set("hbase.zookeeper.property.clientPort" , clientPort);
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
           throw new IllegalArgumentException("connection failed " + e.getMessage() , e) ;
        }
        try {
            admin = connection.getAdmin() ;
        } catch (IOException e) {
            throw new IllegalArgumentException("admin failed " + e.getMessage() , e) ;
        }
    }

    @Override
    public void closeInputFormat()  {
        if (connection != null ){
            try {
                connection.close();
            } catch (IOException e) {
                throw new IllegalArgumentException("connection closed  failed " + e.getMessage() , e) ;
            }
        }
        if (admin != null ){
            try {
                admin.close();
            } catch (IOException e) {
                throw new IllegalArgumentException("admin closed  failed " + e.getMessage() , e) ;
            }
        }

    }

    @Override
    public void open(InputSplit split) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scanner = table.getScanner(scan);
        System.out.println("scanner");
    }

    @Override
    public boolean reachedEnd() throws IOException {
        if (scanner.next() != null) {return true;}
        return false ;
    }

    @Override
    public ResultScanner nextRecord(ResultScanner reuse) throws IOException {
        return reuse;
    }

    @Override
    public void close() throws IOException {
        if (scanner == null ){
            return ;
        }
        try {
            scanner.close();
        }catch (Exception e){
            LOG.info("Inputformat ResultScanner couldn't be closed - " + e.getMessage());
        }
    }
    @VisibleForTesting
    Admin getAdmin(){ return admin ;}
    @VisibleForTesting
    Connection getConnection(){return connection ;}
    public static HBaseInputFormatBuilder buildHBaseInputFormat() {
        return new HBaseInputFormatBuilder();
    }

    public static class HBaseInputFormatBuilder {
        private final HBaseInputFormat format ;

        public HBaseInputFormatBuilder(){
            this.format = new HBaseInputFormat();
        }

        public HBaseInputFormatBuilder setQuorum(String quorum) {
            format.quorum = quorum ;
            return this;
        }
        public HBaseInputFormatBuilder setClientPort(String clientPort){
            format.clientPort = clientPort ;
            return this ;
        }
        public HBaseInputFormatBuilder setTableName(String tableName){
            format.tableName = tableName ;
            return this ;
        }
        public HBaseInputFormatBuilder openInput(){
            format.openInputFormat();
            return this ;
        }

        public HBaseInputFormat finish(){
            if (format.quorum == null ){
                LOG.info("quorum was not supplied separately.");
            }
            if (format.clientPort == null ){
                LOG.info("clientPort was not supplied separately.");
            }
            if (format.tableName == null ){
                LOG.info("tableName was not supplied separately.");
            }
            if (format.connection == null ){
                throw new IllegalArgumentException("No connection supplied");
            }
            if (format.admin == null ){
                throw new IllegalArgumentException("No admin supplied");
            }
            return format ;
        }

    }
}
