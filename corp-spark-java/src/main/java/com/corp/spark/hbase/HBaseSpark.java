package com.corp.spark.hbase;

import com.corp.spark.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.corp.spark.hbase
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/8/30 8:45
 */
public class HBaseSpark {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName(Thread.currentThread() .getStackTrace()[1].getClassName())
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // 创建Spark的Java的执行环境对象
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        // 定义一个累加器
        final Accumulator<Integer> integerAccumulator = context.intAccumulator(0 , "male num ");
        // final Accumulator<Integer> accumulator = context.accumulator(0);
        HBaseUtil.setConf("hadoop101,hadoop102,hadoop103" , "2181");
        // 要扫描的簇
        Scan scan = HBaseUtil.getScan("info") ;
        // 全表扫描得到值
        Configuration configuration = HBaseUtil.getScanConf(scan , "student" ) ;

        // 得到我们从HBase获取到的数据
        JavaPairRDD<ImmutableBytesWritable, Result> resultRDD = context.newAPIHadoopRDD(
                configuration,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class
        );
        // 直接对rdd 做foreach 操作
//        resultRDD.foreach(new VoidFunction<Tuple2<ImmutableBytesWritable, Result>>() {
//            @Override
//            public void call(Tuple2<ImmutableBytesWritable, Result> tuple2) throws Exception {
//
//            }
//        });

        List<Tuple2<ImmutableBytesWritable, Result>> collect = resultRDD.collect();
        Iterator<Tuple2<ImmutableBytesWritable, Result>> iterator = collect.iterator();
        System.out.println("使用迭代器...");
        while (iterator.hasNext()){
            Tuple2<ImmutableBytesWritable, Result> result = iterator.next();
            String row = Bytes.toString(result._2().getRow());
            String value = Bytes.toString(result._2().getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
            String sex = Bytes.toString(result._2().getValue(Bytes.toBytes("info"), Bytes.toBytes("sex")));
            if ("male".equals(sex)){
                integerAccumulator.add(1);
            }
            System.out.println(row + "," + value);
        }
        System.out.println("使用for循环...");
        for (Tuple2<ImmutableBytesWritable, Result> result : collect) {
            // 第一条数据 31 30 30 32
            // 第二条数据 keyvalues={1002/info:age/1566971205948/Put/vlen=2/seqid=0, 1002/info:name/1566971181729/Put/vlen=5/seqid=0, 1002/info:sex/1566971195574/Put/vlen=6/seqid=0}
           // System.out.println(result._1() + "\t" + result._2() + "\n");
            String row = Bytes.toString(result._2().getRow());
            String value = Bytes.toString(result._2().getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
            System.out.println(row + "," + value);

        }
        /**
         * JavaPairRDD 和 JavaRDD
         */
        JavaRDD<Tuple2<ImmutableBytesWritable, Result>> rdd = JavaRDD.fromRDD(JavaPairRDD.toRDD(resultRDD), resultRDD.classTag());

        Iterator<Tuple2<ImmutableBytesWritable, Result>> iterRDD = rdd.collect().iterator();
        while (iterRDD.hasNext()) {
            Tuple2<ImmutableBytesWritable, Result> next = iterRDD.next();
            Iterator<Cell> it = next._2().listCells().iterator();
            while (it.hasNext()) {
                Cell cell = it.next();
                System.out.println(" JavaRDD" + Bytes.toString(cell.getValueArray()));
            }
        }


        /**
         * 得到一个Person类对象聚合而成的map
         */
        JavaRDD<Person> map = resultRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Person>() {
            @Override
            public Person call(Tuple2<ImmutableBytesWritable, Result> tuple2) throws Exception {
                Person person = new Person() ;
                person.setName(Bytes.toString(tuple2._2().getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
                person.setAge(Bytes.toString(tuple2._2().getValue(Bytes.toBytes("info"), Bytes.toBytes("age"))));
                person.setSex(Bytes.toString(tuple2._2().getValue(Bytes.toBytes("info"), Bytes.toBytes("sex"))));
                System.out.println(person.getName());
                return person;
            }
        });
        // 对拿取到的数据做过滤
        List<Person> collectPerson = map.filter(new Function<Person, Boolean>() {
            @Override
            public Boolean call(Person person) throws Exception {
                if (Integer.parseInt(person.getAge()) > 19) {
                    return true;
                }
                return false;
            }
        }).collect();

        for (Person person : collectPerson) {
            System.out.println("过滤后" + person.toString());
        }
        System.out.println("male 的累加器：" +integerAccumulator.value());

        context.stop();
    }
}




class Person{
    String name ;
    String age ;
    String sex ;

    public Person(String name, String age, String sex) {
        this.name = name;
        this.age = age;
        this.sex = sex;
    }

    public Person() {
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age='" + age + '\'' +
                ", sex='" + sex + '\'' +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }
}