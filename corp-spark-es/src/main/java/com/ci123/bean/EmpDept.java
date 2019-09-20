package com.ci123.bean;

import org.apache.spark.sql.Row;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.bean
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/9 17:42
 */
public class EmpDept {
    private String name;
    private String email;
    private String company;
    private String address;
    private String dept_name;
    private String dept_addr;

    public EmpDept(String name, String email, String company, String address, String dept_name, String dept_addr) {
        this.name = name;
        this.email = email;
        this.company = company;
        this.address = address;
        this.dept_name = dept_name;
        this.dept_addr = dept_addr;
    }

    public EmpDept() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getDept_name() {
        return dept_name;
    }

    public void setDept_name(String dept_name) {
        this.dept_name = dept_name;
    }

    public String getDept_addr() {
        return dept_addr;
    }

    public void setDept_addr(String dept_addr) {
        this.dept_addr = dept_addr;
    }

    public static EmpDept parseEmpDept(Row row){

        return null ;
    }
}
