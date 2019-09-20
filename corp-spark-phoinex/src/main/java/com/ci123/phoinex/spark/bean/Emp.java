package com.ci123.phoinex.spark.bean;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.phoinex.spark.bean
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/9 15:59
 */
public class Emp {
    private String empid ;
    private String name ;
    private String age ;
    private String sex ;
    private String email ;
    private String company ;
    private String address ;
    private String deptid ;

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

    public Emp() {
    }

    public String getEmpid() {
        return empid;
    }

    public void setEmpid(String empid) {
        this.empid = empid;
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

    public String getDeptid() {
        return deptid;
    }

    public void setDeptid(String deptid) {
        this.deptid = deptid;
    }
}
