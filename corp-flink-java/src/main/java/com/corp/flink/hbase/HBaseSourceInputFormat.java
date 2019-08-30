package com.corp.flink.hbase;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.corp.flink.hbase
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/8/28 14:50
 */
public class HBaseSourceInputFormat implements InputFormat<String, InputSplit> {
    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        return null;
    }

    @Override
    public InputSplit[] createInputSplits(int i) throws IOException {
        return new InputSplit[0];
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return null;
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {

    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public String nextRecord(String s) throws IOException {
        return null;
    }


    @Override
    public void close() throws IOException {

    }
}
