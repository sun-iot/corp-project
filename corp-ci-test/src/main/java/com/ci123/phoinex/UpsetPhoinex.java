package com.ci123.phoinex;

import java.sql.SQLException;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.phoinex
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/10/11 17:15
 */
public class UpsetPhoinex {
    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {

        //upsert into employee values(1570761939935 ,'emp_020','Alan020','20','male','emp_020@corp-ci.com' ,'深空视线科技','南京');

        new Thread(new EmployeeThread()).start();
        new Thread(new EmployeeThread2()).start();
    }



    public static void dept() throws InterruptedException {
        String deptId = "dept_000" ;
        String deptName = "deptName_000" ;
        String deptManager = "deptManager_000" ;
        String format = "" ;
        String[] deptAddr = new String[]{"1402","1401","1403","1404","1405","1406","1407","1408","1409"};
        String[] sex = new String[]{"男", "女"} ;
        for (int i = 100; i < 108; i++) {
            long timestamp = System.currentTimeMillis();
            deptId = String.format("dept_0%s", i);
            deptName = String.format("deptName_%s", i);
            deptManager = String.format("deptManager_%s" , i) ;
            int position = (int)(Math.random()*8 + 0) ;
            format = String.format("upsert into development values(%s ,'%s','%s','%s' ,'%s');",
                    timestamp, deptId, deptName, deptManager , deptAddr[position]);
            Thread.sleep(1500);
            System.out.println(format);
        }
    }
}
/**
 CREATE TABLE IF NOT EXISTS employee (
 "__time" BIGINT NOT NULL,
 "empId" VARCHAR NOT NULL,
 "username" VARCHAR,
 "age" BIGINT,
 "sex" VARCHAR,
 "email" VARCHAR,
 "company" VARCHAR
 CONSTRAINT emp_pk PRIMARY KEY ("__time", "empId"));

 CREATE TABLE IF NOT EXISTS development (
 "__time" BIGINT NOT NULL,
 "deptId" VARCHAR NOT NULL,
 "deptName" VARCHAR,
 "deptManager" VARCHAR,
 "deptAddr" VARCHAR
 CONSTRAINT emp_pk PRIMARY KEY ("__time", "deptId"));


 **/

class EmployeeThread extends Thread {
    String id = "emp_000" ;
    String name = "Alan000" ;
    String email = "emp_020@corp-ci.com" ;
    String format = "" ;

    String[] companies = new String[]{"蓝海星科技","致一科技","环球科技","惠申科技","育儿网科技","神图科技","123科技","领航员科技","指南针科技"};
    String[] addresses = new String[]{"北京","上海","南京","深圳","杭州","合肥","芜湖","日照","青岛"};
    String[] sex = new String[]{"男", "女"} ;
    @Override
    public void run() {
        for (int i = 200; i < 250; i++) {
            long timestamp = System.currentTimeMillis();
            id = String.format("emp_0%s", i);
            name = String.format("Alan_0%s", i);
            email = String.format("emp_0%s@corp-ci.com" , i) ;
            int position = (int)(Math.random()*8 + 0) ;
            int age = (int)(Math.random()*10 + 20);
            format = String.format("upsert into employee values(%s ,'%s','%s',%s,'%s' ,'%s','%s');",
                    timestamp, id, name, age , sex[i%2] ,email , companies[position] , addresses[position]);
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(format);
        }

    }
}
class EmployeeThread2 extends Thread {
    String id = "emp_000" ;
    String name = "Alan000" ;
    String email = "emp_020@corp-ci.com" ;
    String format = "" ;

    String[] companies = new String[]{"蓝海星科技","致一科技","环球科技","惠申科技","育儿网科技","神图科技","123科技","领航员科技","指南针科技"};
    String[] addresses = new String[]{"北京","上海","南京","深圳","杭州","合肥","芜湖","日照","青岛"};
    String[] sex = new String[]{"男", "女"} ;
    @Override
    public void run() {
        for (int i = 150; i < 200; i++) {
            long timestamp = System.currentTimeMillis();
            id = String.format("emp_0%s", i);
            name = String.format("Alan_0%s", i);
            email = String.format("emp_0%s@corp-ci.com" , i) ;
            int position = (int)(Math.random()*8 + 0) ;
            int age = (int)(Math.random()*10 + 20);
            format = String.format("upsert into employee values(%s ,'%s','%s',%s,'%s' ,'%s','%s');",
                    timestamp, id, name, age , sex[i%2] ,email , companies[position] , addresses[position]);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(format);
        }

    }
}