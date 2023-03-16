package com.galaxy.tsg.function;

import com.galaxy.tsg.pojo.Entity;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple1;

import java.util.HashMap;

public class metricsAggregation implements AggregateFunction<Entity, Tuple1<HashMap<String, Entity>>, Tuple1<HashMap<String, Entity>>> {

    private final int topSize;

    public metricsAggregation(int i) {

        this.topSize = i;

    }

    @Override
    public Tuple1<HashMap<String, Entity>> createAccumulator() {


        return Tuple1.of(new HashMap<String, Entity>());
    }

    @Override
    public Tuple1<HashMap<String, Entity>> add(Entity value, Tuple1<HashMap<String, Entity>> accumulator) {


        if(accumulator.f0.containsKey(value.getKey_by())) {

            Entity value1= accumulator.f0.get(value.getKey_by());
            value1.setCommon_c2s_pkt_num(value1.getCommon_c2s_pkt_num() + value.getCommon_c2s_pkt_num());
            value1.setCommon_s2c_pkt_num(value1.getCommon_s2c_pkt_num() + value.getCommon_s2c_pkt_num());
            value1.setCommon_c2s_byte_num(value1.getCommon_c2s_byte_num() + value.getCommon_c2s_byte_num());
            value1.setCommon_s2c_byte_num(value1.getCommon_s2c_byte_num() + value.getCommon_s2c_byte_num());
            value1.setCommon_sessions(value1.getCommon_sessions() + value.getCommon_sessions());
        }
        else {
            if(accumulator.f0.size()<topSize){
                accumulator.f0.put(value.getKey_by(),value);
            }

        }
        return Tuple1.of(accumulator.f0);
    }

    @Override
    public Tuple1<HashMap<String, Entity>> getResult(Tuple1<HashMap<String, Entity>> accumulator) {
        return  accumulator;
    }

    @Override
    public Tuple1<HashMap<String, Entity>> merge(Tuple1<HashMap<String, Entity>> a, Tuple1<HashMap<String, Entity>> b) {
        return null;
    }
}
