package com.ci123.bean;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.bean
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/17 16:35
 */
public
class Emp {
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

}
