package com.corp.spark.hbase;

import com.corp.spark.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 *     通过使用hbase-spark的连接器，建立起HBase和Spark之间的连接
 * Project: corp-project
 * Package: com.corp.spark.hbase
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/8/30 11:16
 */
public class HBaseSparkConnector {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName(Thread.currentThread() .getStackTrace()[1].getClassName())
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");;
        // 创建Spark的Java的执行环境对象
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        HBaseUtil.setConf("hadoop101,hadoop102,hadoop103" , "2181");
        // 要扫描的簇
        Scan scan = HBaseUtil.getScan("info" , "name" , "sex","age") ;

        // 再来一个family
        scan = HBaseUtil.getScan(scan , "grade" , "Math");
        // 全表扫描得到值
        Configuration configuration = HBaseUtil.getScanConf(scan , "student" ) ;

        JavaPairRDD<ImmutableBytesWritable, Result> resultRDD = context.newAPIHadoopRDD(
                configuration,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class

        );

        resultRDD.foreach(new VoidFunction<Tuple2<ImmutableBytesWritable, Result>>() {
            @Override
            public void call(Tuple2<ImmutableBytesWritable, Result> tuple2) throws Exception {
                System.out.println(tuple2._2());
//                Iterator<Cell> iterator = tuple2._2().listCells().iterator();
//                while (iterator.hasNext()) {
//                    Cell cell = iterator.next();
//                    String value = Bytes.toString(cell.getValueArray());
//                }
            }
        });

        context.stop();
    }
}

