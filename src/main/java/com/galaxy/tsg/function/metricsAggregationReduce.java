package com.galaxy.tsg.function;

import com.galaxy.tsg.pojo.Entity;
import org.apache.flink.api.common.functions.ReduceFunction;

public class metricsAggregationReduce implements ReduceFunction<Entity> {

    @Override
    public Entity reduce(Entity value1, Entity value2) throws Exception {
        value1.setCommon_c2s_pkt_num(value1.getCommon_c2s_pkt_num() + value2.getCommon_c2s_pkt_num());
        value1.setCommon_s2c_pkt_num(value1.getCommon_s2c_pkt_num() + value2.getCommon_s2c_pkt_num());
        value1.setCommon_c2s_byte_num(value1.getCommon_c2s_byte_num() + value2.getCommon_c2s_byte_num());
        value1.setCommon_s2c_byte_num(value1.getCommon_s2c_byte_num() + value2.getCommon_s2c_byte_num());
        value1.setCommon_sessions(value1.getCommon_sessions()+value2.getCommon_sessions());

        return value1;
    }
}
