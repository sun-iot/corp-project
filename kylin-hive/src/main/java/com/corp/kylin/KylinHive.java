package com.corp.kylin;

import org.apache.kylin.jdbc.Driver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class KylinHive {
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {

        Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
        Properties properties = new Properties();
        properties.put("user", "ADMIN");
        properties.put("password", "KYLIN");
        Connection connect = driver.connect("jdbc:kylin://hadoop101:7070/hive_kylin_first_demo", properties);
        String sql = "select dname,sum(sal) from emp join dept on emp.deptno=dept.deptno group by dname;";
        ResultSet resultSet = connect.prepareStatement(sql).executeQuery();
        while (resultSet.next()) {
            System.out.println(resultSet.getString("dname") + "\t" + resultSet.getDouble(2));
        }
    }
}
