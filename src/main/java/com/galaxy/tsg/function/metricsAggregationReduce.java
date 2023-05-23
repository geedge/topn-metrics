package com.galaxy.tsg.function;

import com.galaxy.tsg.pojo.transformEntity;
import org.apache.flink.api.common.functions.ReduceFunction;

public class metricsAggregationReduce implements ReduceFunction<transformEntity> {

    @Override
    public transformEntity reduce(transformEntity value1, transformEntity value2) throws Exception {
        value1.setOut_pkts(value1.getOut_pkts() + value2.getOut_pkts());
        value1.setOut_bytes(value1.getOut_bytes() + value2.getOut_bytes());
        value1.setIn_bytes(value1.getIn_bytes() + value2.getIn_bytes());
        value1.setIn_pkts(value1.getIn_pkts() + value2.getIn_pkts());
        value1.setSessions(value1.getSessions()+value2.getSessions());
        return value1;
    }
}
