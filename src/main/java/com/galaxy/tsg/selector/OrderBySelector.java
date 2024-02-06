package com.galaxy.tsg.selector;

import com.galaxy.tsg.pojo.ResultEntity;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;

public class OrderBySelector implements KeySelector<ResultEntity, Tuple1<String>> {

    @Override
    public Tuple1<String> getKey(ResultEntity entity) throws Exception {
        return new Tuple1<>(entity.getOrder_by());
    }
}
