package com.ci123.flink.elasticsearch;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RuntimeContext;

public interface ElasticsearchSinkFunction<T> extends Function {


    void process(T element, RuntimeContext ctx, RequestIndexer requestIndexer);

}
