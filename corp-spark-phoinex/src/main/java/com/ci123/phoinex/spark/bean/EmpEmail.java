package com.ci123.phoinex.spark.bean;

import org.apache.spark.sql.Row;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.phoinex.spark.bean
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/9 16:19
 */
public class EmpEmail {

    private String name ;
    private String eamil ;

    public EmpEmail(String name, String eamil) {
        this.name = name;
        this.eamil = eamil;
    }

    public EmpEmail() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEamil() {
        return eamil;
    }

    public void setEamil(String eamil) {
        this.eamil = eamil;
    }

    public static EmpEmail pasreEmp(Row row){
        String emp = row.getString(1);

        return new EmpEmail(row.get(0).toString() , row.get(1).toString());
    }
}
