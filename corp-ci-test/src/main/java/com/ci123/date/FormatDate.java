package com.ci123.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.date
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/10/10 15:35
 */
public class FormatDate {

    private static  SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    // 传入一个时间戳，并根据所传入的时间戳判断时间戳是否包含 ms ，s , h 即，
    private static String getDateTime(long timestamp){
        return DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault()));
    }
    // 将得到的时间 转化成 timestamp
    private static long getTimestamp(String dateTime){
        System.out.println("getTimestamp:" + dateTime);
        return LocalDateTime.from(LocalDateTime.parse(dateTime, DateTimeFormatter.ofPattern("yyyy-MM-dd"))).
                atZone(ZoneId.systemDefault()).
                toInstant().toEpochMilli();
    }

    /*
     * 将时间转换为时间戳
     */
    public static String dateToStamp(String s) throws ParseException {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = simpleDateFormat.parse(s);
        long ts = date.getTime();
        res = String.valueOf(ts);
        return res;
    }

    public static void main(String[] args) throws ParseException {


        for (int i = 0; i < 10 ; i++) {
            try {
                Long timeStamp = System.currentTimeMillis();  //获取当前时间戳
                System.out.println(timeStamp);
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }
}
