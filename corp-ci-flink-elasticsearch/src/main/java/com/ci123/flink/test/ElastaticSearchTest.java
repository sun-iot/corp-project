package com.ci123.flink.test;

import com.alibaba.fastjson.JSONObject;
import com.ci123.flink.elasticsearch.ElasticsearchApiCallBridge;
import com.ci123.flink.elasticsearch.ElasticsearchOutputFormat;
import com.ci123.flink.elasticsearch.ElasticsearchSinkFunction;
import com.ci123.flink.elasticsearch.RequestIndexer;
import com.ci123.flink.util.ConfigConstant;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import scala.Tuple8;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.flink.test
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/18 11:09
 */
public class ElastaticSearchTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        TypeInformation[] fieldTypes = new TypeInformation[]{
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO

        };
        // 写入到ES的配置
        Map<String, String> config = new HashMap<>();
        config.put("bulk.flush.max.actions", ConfigConstant.getVal("es.max.actions"));
        config.put("cluster.name", ConfigConstant.getVal("es.cluster.name"));
        String hosts = ConfigConstant.getVal("es.node.host");
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);

        JDBCInputFormat JDBC = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("org.apache.phoenix.jdbc.PhoenixDriver")
                .setDBUrl("jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181")
                .setFetchSize(1024)
                .setQuery("select * from emp ")
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        DataSet<Row> hbaseSet = env.createInput(JDBC);
        // 配置批式环境 TableEnvironment
        MapOperator<Row, Tuple8<String, String, String, String, String, String, String, String>> map = hbaseSet.map(new MapFunction<Row, Tuple8<String, String, String, String, String, String, String, String>>() {
            @Override
            public Tuple8<String, String, String, String, String, String, String, String> map(Row value) throws Exception {
                String strValue = value.toString();
                // System.out.println(strValue);
                String[] split = strValue.split(",");
                return new Tuple8<>(split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7]);
            }
        });

        List<InetSocketAddress> list = Lists.newArrayList();
        for (String host : hosts.split(",")) {
            list.add(new InetSocketAddress(InetAddress.getByName(host), 9300));
        }

        Map<String, String> esIndexType = new HashMap<>();
        esIndexType.put("es-flink-index", "test_index");
        esIndexType.put("es-flink-type", "test_type");

        final List<HttpHost> httpHosts = Arrays.asList(hosts.split(","))
                .stream()
                .map(host -> new HttpHost(host, 9200, "http"))
                .collect(Collectors.toList());

        map.output((new ElasticsearchOutputFormat.Builder(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple8<String, String, String, String, String, String, String, String>>() {
                    @Override
                    public void process(Tuple8<String, String, String, String, String, String, String, String> element, RuntimeContext ctx, RequestIndexer requestIndexer) {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("empid", element._1());
                        jsonObject.put("name", element._2());
                        jsonObject.put("age", element._3());
                        jsonObject.put("sex", element._4());
                        jsonObject.put("email", element._5());
                        jsonObject.put("company", element._6());
                        jsonObject.put("address", element._7());
                        jsonObject.put("depyid", element._8());

                        requestIndexer.add(createIndexRequest(jsonObject, ParameterTool.fromMap(esIndexType)));

                    }
                    private IndexRequest createIndexRequest(JSONObject element, ParameterTool parameterTool) {

                        return Requests.indexRequest()
                                .index(parameterTool.getRequired("es-flink-index"))
                                .type(parameterTool.getRequired("es-flink-type"))
                                .source(element);
                    }
                }
        ).setBulkFlushBackoff(true)
                .setBulkFlushBackoffRetries(2)
                .setBulkFlushBackoffType(ElasticsearchApiCallBridge.FlushBackoffType.EXPONENTIAL)
                .setBulkFlushMaxSizeMb(1024)
                .setBulkFlushInterval(1024*1024)
                .setBulkFlushMaxActions(512)
                .build()
        ));

        hbaseSet.print();
        map.print();

    }
}
