package com.corp.flink.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.corp.flink.util
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/8/28 11:35
 */
public class HBaseUtil {
    public static Configuration conf = null;
    public static Connection connection = null;
    public static Admin admin = null;
    /**
     * @desc 取得连接
     */
    public static void setConf(String quorum, String port) {
        try {
            System.setProperty("hadoop.home.dir", "G:\\development\\hadoop-2.7.2");
            conf = HBaseConfiguration.create();
            // zookeeper地址
            conf.set("hbase.zookeeper.quorum", quorum);
            conf.set("hbase.zookeeper.property.clientPort", port);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 连接关闭
     */
    public static void close() {
        try {
            if (connection != null) {
                connection.close();
            }
            if (admin != null) {
                admin.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 追加 family
     * @param tableName
     * @param family
     */
    public static void addFaimily(String tableName , String family){
        TableName taName = TableName.valueOf(tableName);
        try {
            admin.disableTable(taName);
            // 得到表的描述器
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(taName);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(hColumnDescriptor);
            admin.modifyTable(taName , tableDescriptor);
            admin.enableTable(taName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     * @param tableName
     * @param columnFamily
     */
    public static void createTable(String tableName, String columnFamily) {
        try {
            TableName tbName = TableName.valueOf(tableName);
            if (!admin.tableExists(tbName)) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tbName);
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
                hTableDescriptor.addFamily(hColumnDescriptor);
                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 添加多条记录
     * @param tableName
     * @param family
     * @param qualifier
     * @param rowList
     * @param value
     */
    public static void addMoreRecord(String tableName, String family, String  qualifier, List < String > rowList, String value){
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            List<Put> puts = new ArrayList<>();
            Put put = null;
            for (int i = 0; i < rowList.size(); i++) {
                put = new Put(Bytes.toBytes(rowList.get(i)));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                puts.add(put);
            }
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 查询rowkey下某一列值
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @return
     */
    public static String getValue(String tableName, String rowKey, String family, String qualifier) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            //返回指定列族、列名，避免rowKey下所有数据
            get.addColumn(family.getBytes(), qualifier.getBytes());
            Result rs = table.get(get);
            Cell cell = rs.getColumnLatestCell(family.getBytes(), qualifier.getBytes());
            String value = null;
            if (cell!=null) {
                value = Bytes.toString(CellUtil.cloneValue(cell));
            }
            return value;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * 查询rowkey下多列值，一起返回
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @return String[]
     */
    public  String[] getQualifierValue(String tableName, String rowKey, String family, String[] qualifier) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            //返回指定列族、列名，避免rowKey下所有数据
            get.addColumn(family.getBytes(), qualifier[0].getBytes());
            get.addColumn(family.getBytes(), qualifier[1].getBytes());
            Result rs = table.get(get);
            // 返回最新版本的Cell对象
            Cell cell = rs.getColumnLatestCell(family.getBytes(), qualifier[0].getBytes());
            Cell cell1 = rs.getColumnLatestCell(family.getBytes(), qualifier[1].getBytes());
            String[] value = new String[qualifier.length];
            if (cell!=null) {
                value[0] = Bytes.toString(CellUtil.cloneValue(cell));
                value[1] = Bytes.toString(CellUtil.cloneValue(cell1));
            }
            return value;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    /**
     * 获取一行数据
     * @param tableName 表名
     * @param rowKey 唯一标识
     * @param family 簇名
     * @return list
     */
    public static List<Cell> getRowCells(String tableName, String rowKey, String family) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            get.addFamily(family.getBytes());
            Result rs = table.get(get);
            List<Cell> cellList =   rs.listCells();
//    		如果需要,遍历cellList
//    		if (cellList!=null) {
//    			String qualifier = null;
//    			String value = null;
//    			for (Cell cell : cellList) {
//    				qualifier = Bytes.toString( cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
//    				value = Bytes.toString( cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
//    				System.out.println(qualifier+"--"+value);
//    			}
//			}
            return cellList;
        } catch (IOException e) {
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return null;
    }
    /**
     * 全表扫描
     * @param tableName
     * @return
     */
    public static ResultScanner scan(String tableName, String family, String qualifier) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner rs = table.getScanner(scan);
//			一般返回ResultScanner，遍历即可
//			if (rs!=null){
//				String row = null;
//				String quali = null;
//    			String value = null;
//				for (Result result : rs) {
//					row = Bytes.toString(CellUtil.cloneRow(result.getColumnLatestCell(family.getBytes(), qualifier.getBytes())));
//					quali =Bytes.toString(CellUtil.cloneQualifier(result.getColumnLatestCell(family.getBytes(), qualifier.getBytes())));
//					value =Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(family.getBytes(), qualifier.getBytes())));
//					System.out.println(row+"-"+quali+"-"+value);
//				}
//			}
            return rs;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * 全表扫描
     * @param tableName 表名
     * @return
     */
    public static ResultScanner scan(String tableName) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner rs = table.getScanner(scan);
            return rs;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}