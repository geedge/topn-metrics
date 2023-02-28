package com.galaxy.tsg.function;

import com.galaxy.tsg.pojo.UrlEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

public class UrlAggregationReduce implements ReduceFunction<UrlEntity> {

    @Override
    public UrlEntity reduce(UrlEntity value1, UrlEntity value2) throws Exception {
        value1.setCommon_sessions(value1.getCommon_sessions()+value2.getCommon_sessions());
        return value1;
    }
}
