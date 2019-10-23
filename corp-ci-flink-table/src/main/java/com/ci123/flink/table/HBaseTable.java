package com.ci123.flink.table;

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
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.Tumble;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.flink.table
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/18 11:19
 */
public class HBaseTable {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Row> sourceHBase = env.createInput(new JDBCInputFormat().buildJDBCInputFormat()
                .setDrivername("org.apache.phoenix.jdbc.PhoenixDriver")
                .setDBUrl("jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181")
                .setFetchSize(1024)
                .setQuery("select * from emp ")
                .setRowTypeInfo(new RowTypeInfo(new TypeInformation[] {
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                }))
                .finish());
        DataSet<Emp> empDataSet = sourceHBase.map(new MapFunction<Row, Emp>() {
            @Override
            public Emp map(Row value) throws Exception {
                List<Emp> list = new ArrayList<Emp>();
                String strEmp = value.toString();
                String[] split = strEmp.split(",");
                Emp emp =  new Emp(split[0], split[1], split[2], split[3], split[4], split[5], split[6], split[7]) ;
                return emp ;
            }
        });
        // 第二张数据源表
        DataSource<Row> sourceDeptHBase = env.createInput(new JDBCInputFormat().buildJDBCInputFormat()
                .setDrivername("org.apache.phoenix.jdbc.PhoenixDriver")
                .setDBUrl("jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181")
                .setFetchSize(1024)
                .setQuery("select * from dept ")
                .setRowTypeInfo(new RowTypeInfo(new TypeInformation[] {
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO
                }))
                .finish());
        sourceHBase.print();
        DataSet<Dept> deptDataSet = sourceDeptHBase.map(new MapFunction<Row, Dept>() {
            @Override
            public Dept map(Row value) throws Exception {
                String strEmp = value.toString();
                String[] split = strEmp.split(",");
                Dept dept =  new Dept(split[0], split[1], split[2], split[3]) ;
                return dept ;
            }
        });

        // 定义Table的环境变量
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        // 将dataSet转化为table
        Table tableEmp = tableEnv.fromDataSet(empDataSet, "empid,name,age,sex,email,company,address,deptid");
        Table tableDept = tableEnv.fromDataSet(deptDataSet, "deptId,deptName,deptManager,deptAddr");

        // 注册tableEmp
        tableEnv.registerTable("Emp" , tableEmp);
        tableEnv.registerTable("Dept" , tableDept);

        // 扫描整张表，查询字段
        Table enmpTable = tableEnv.scan("Emp").as("empid,name,age,sex,email,company,address,deptid");
        Table deptTable = tableEnv.scan("Dept").as("id,deptName,deptManager,deptAddr");
        Table selectNameDeptidEmail = enmpTable.select("name,age,sex,email,company");
        Table selectLeftJoinEmpDept = enmpTable.join(deptTable).where("deptid=id ").select("name,age,sex,email,company,deptName,deptManager");

        enmpTable.window(Tumble.over("3.minutes").on("rowTime").as("empWindow"));

        enmpTable.select("name,age,sex,email,company").groupBy("name,age,sex,email,company");

        // 把得到的新表，转化成Dataset
        DataSet<Result> resultDataSet = tableEnv.toDataSet(selectNameDeptidEmail, Result.class);
        DataSet<ResultEmpDept> resultEmpept = tableEnv.toDataSet(selectLeftJoinEmpDept, ResultEmpDept.class);


        // 写入到ES的配置
        Map<String, String> config = new HashMap<>();
        config.put("bulk.flush.max.actions", ConfigConstant.getVal("es.max.actions"));
        config.put("cluster.name", ConfigConstant.getVal("es.cluster.name"));

        String hosts = ConfigConstant.getVal("es.node.host");

        Map<String, String> esIndexType = new HashMap<>();
        esIndexType.put("index", "flink-index");
        esIndexType.put("type", "flink-type");

        final List<HttpHost> httpHosts = Arrays.asList(hosts.split(","))
                .stream()
                .map(host -> new HttpHost(host, 9200, "http"))
                .collect(Collectors.toList());

        resultDataSet.output( new ElasticsearchOutputFormat.Builder(
                httpHosts,
                new ElasticsearchSinkFunction<Result>() {
                    @Override
                    public void process(Result element, RuntimeContext ctx, RequestIndexer requestIndexer) {
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("name" , element.name) ;
                        jsonObject.put("age" , element.age) ;
                        jsonObject.put("sex" , element.sex) ;
                        jsonObject.put("email" , element.email) ;
                        jsonObject.put("company" , element.company) ;
                        requestIndexer.add(createIndexRequest(jsonObject , ParameterTool.fromMap(esIndexType)));
                    }
                    private IndexRequest createIndexRequest(JSONObject element, ParameterTool parameterTool) {
                        System.out.println(element.toJSONString());
                        return Requests.indexRequest()
                                .index("flink_index")
                                .type("flink_type")
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

        );


        // 对得到的结果集，打印对象
//        resultDataSet.print();

    }


    public static class Emp {
        public String empid ;
        public String name ;
        public String age ;
        public String sex ;
        public String email ;
        public String company ;
        public String address ;
        public String deptid ;

        public Emp() {
            super();
        }

        public Emp(String empid, String name, String age, String sex, String email, String company, String address, String deptid) {
            this.empid = empid;
            this.name = name;
            this.age = age;
            this.sex = sex;
            this.email = email;
            this.company = company;
            this.address = address;
            this.deptid = deptid;
        }
    }

    public static class Result {
        public String name ;
        public String age ;
        public String sex ;
        public String email ;
        public String company ;

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

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public String getCompany() {
            return company;
        }

        public void setCompany(String company) {
            this.company = company;
        }

        public Result() {
            super();
        }

        @Override
        public String toString() {
            return "Result{" +
                    "name='" + name + '\'' +
                    ", age='" + age + '\'' +
                    ", sex='" + sex + '\'' +
                    ", email='" + email + '\'' +
                    ", company='" + company + '\'' +
                    '}';
        }
    }
    // name,age,sex,email,company,deptName,deptManager
    public static class ResultEmpDept {
        public String name ;
        public String age ;
        public String sex ;
        public String email ;
        public String company ;
        public String deptName ;
        public String deptManager ;



        public ResultEmpDept() {
            super();
        }

        @Override
        public String toString() {
            return "ResultEmpDept{" +
                    "name='" + name + '\'' +
                    ", age='" + age + '\'' +
                    ", sex='" + sex + '\'' +
                    ", email='" + email + '\'' +
                    ", company='" + company + '\'' +
                    ", deptName='" + deptName + '\'' +
                    ", deptManager='" + deptManager + '\'' +
                    '}';
        }
    }

    public static class Dept{
        public String deptId ;
        public String deptName ;
        public String deptManager ;
        public String deptAddr ;

        public Dept() {
        }

        public Dept(String deptId, String deptName, String deptManager, String deptAddr) {
            this.deptId = deptId;
            this.deptName = deptName;
            this.deptManager = deptManager;
            this.deptAddr = deptAddr;
        }
    }
}
