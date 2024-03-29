package com.ci123.flink.elasticsearch;

import org.apache.flink.annotation.PublicEvolving;
import org.elasticsearch.action.DocWriteRequest;

import java.io.Serializable;

/**
 * Copyright (c) 2018-2028 Corp-ci All Rights Reserved
 * <p>
 * Project: corp-project
 * Package: com.ci123.elasticsearch
 * Version: 1.0
 * <p>
 * Created by SunYang on 2019/9/17 14:10
 */
@PublicEvolving
public interface DocWriteRequestFailureHandler extends Serializable {

	/**
	 *
	 * @param action
	 * @param failure
	 * @param restStatusCode
	 * @param indexer
	 * @throws Throwable
	 */
	void onFailure(DocWriteRequest action, Throwable failure, int restStatusCode,
                   RequestIndexer indexer) throws Throwable;

}
