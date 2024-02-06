package com.galaxy.tsg.util;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson2.JSON;
import com.galaxy.tsg.pojo.SessionEntity;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author qidaijie
 * @version 2022/3/89:42
 */
public class TimestampDeserializationSchema implements KafkaDeserializationSchema {
    private static final Log logger = LogFactory.get();

    @Override
    public TypeInformation getProducedType() {
        return TypeInformation.of(new TypeHint<SessionEntity>() {});
    }

    @Override
    public boolean isEndOfStream(Object nextElement) {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public SessionEntity deserialize(ConsumerRecord record) throws Exception {
        if (record != null) {
            try {


                SessionEntity sessionEntity = JSON.parseObject((byte[]) record.value(), SessionEntity.class);
                sessionEntity.setRecv_time(record.timestamp());

                return sessionEntity;
            } catch (RuntimeException e) {
                logger.error(
                        "KafkaConsumer Deserialize failed,The exception is : " + e.getMessage());
                e.printStackTrace();
            }
        }
        return new SessionEntity();
    }
}
