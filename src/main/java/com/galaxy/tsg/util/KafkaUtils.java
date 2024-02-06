package com.galaxy.tsg.util;

import com.galaxy.tsg.config.CommonConfig;
import com.galaxy.tsg.pojo.SessionEntity;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.config.SslConfigs;

import java.util.Optional;
import java.util.Properties;

import static com.galaxy.tsg.config.CommonConfig.KAFKA_CONSUMER_TOPIC;
import static com.galaxy.tsg.config.CommonConfig.KAFKA_PRODUCER_TOPIC;

public class KafkaUtils {


    public static Properties getKafkaSourceProperty(Configuration configuration) {
        Properties properties = new Properties();
        properties.setProperty("group.id", configuration.getString(CommonConfig.KAFKA_CONSUMER_GROUP_ID));
        properties.setProperty("bootstrap.servers",configuration.getString(CommonConfig.KAFKA_CONSUMER_BROKER));
        properties.setProperty("session.timeout.ms", configuration.getString(CommonConfig.KAFKA_CONSUMER_SESSION_TIMEOUT_MS));
        properties.setProperty("max.poll.records", configuration.getString(CommonConfig.KAFKA_CONSUMER_MAX_POLL_RECORD));
        properties.setProperty("max.partition.fetch.bytes", configuration.getString(CommonConfig.KAFKA_CONSUMER_MAX_PARTITION_FETCH_BYTES));


        switch (configuration.getInteger(CommonConfig.KAFKA_CONSUMER_SECURITY)) {
            case 1:
                properties.put("security.protocol", "SSL");
                properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
                properties.put("ssl.keystore.location", configuration.getString(CommonConfig.TOOLS_CONSUMER_LIBRARY) + "keystore.jks");
                properties.put("ssl.keystore.password", configuration.getString(CommonConfig.KAFKA_CONSUMER_PIN));
                properties.put("ssl.truststore.location", configuration.getString(CommonConfig.TOOLS_CONSUMER_LIBRARY) + "truststore.jks");
                properties.put("ssl.truststore.password", configuration.getString(CommonConfig.KAFKA_CONSUMER_PIN));
                properties.put("ssl.key.password", configuration.getString(CommonConfig.KAFKA_CONSUMER_PIN));
                break;
            case 2:
                properties.put("security.protocol", "SASL_PLAINTEXT");
                properties.put("sasl.mechanism", "PLAIN");
                properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username="
                        + configuration.getString(CommonConfig.KAFKA_CONSUMER_USER) + " password=" + configuration.getString(CommonConfig.KAFKA_CONSUMER_PIN) + ";");
                break;
            default:
        }

        return properties;
    }
    private static Properties getKafkaSinkProperty(Configuration configuration) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",configuration.getString(CommonConfig.KAFKA_PRODUCER_BROKER));

        properties.put("acks", "1");
        properties.put("retries", configuration.getString(CommonConfig.KAFKA_PRODUCER_RETRIES));
        properties.put("linger.ms", configuration.getString(CommonConfig.KAFKA_PRODUCER_LINGER_MS));
        properties.put("request.timeout.ms", configuration.getString(CommonConfig.KAFKA_PRODUCER_REQUEST_TIMEOUT_MS));
        properties.put("batch.size", configuration.getString(CommonConfig.KAFKA_PRODUCER_BATCH_SIZE));
        properties.put("buffer.memory", configuration.getString(CommonConfig.KAFKA_PRODUCER_BUFFER_MEMORY));
        properties.put("max.request.size", configuration.getString(CommonConfig.KAFKA_PRODUCER_MAX_REQUEST_SIZE));
        properties.put("compression.type", configuration.getString(CommonConfig.KAFKA_PRODUCER_COMPRESSION_TYPE));
        switch (configuration.getInteger(CommonConfig.KAFKA_PRODUCER_SECURITY)) {
            case 1:
                properties.put("security.protocol", "SSL");
                properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
                properties.put("ssl.keystore.location", configuration.getString(CommonConfig.TOOLS_PRODUCER_LIBRARY) + "keystore.jks");
                properties.put("ssl.keystore.password", configuration.getString(CommonConfig.KAFKA_PRODUCER_PIN));
                properties.put("ssl.truststore.location", configuration.getString(CommonConfig.TOOLS_PRODUCER_LIBRARY) + "truststore.jks");
                properties.put("ssl.truststore.password", configuration.getString(CommonConfig.KAFKA_PRODUCER_PIN));
                properties.put("ssl.key.password", configuration.getString(CommonConfig.KAFKA_PRODUCER_PIN));
                break;
            case 2:
                properties.put("security.protocol", "SASL_PLAINTEXT");
                properties.put("sasl.mechanism", "PLAIN");
                properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username="
                        + configuration.getString(CommonConfig.KAFKA_PRODUCER_USER) + " password=" + configuration.getString(CommonConfig.KAFKA_PRODUCER_PIN) + ";");
                break;
            default:
        }
        return properties;
    }

    public static FlinkKafkaConsumer<SessionEntity> getKafkaConsumer(Configuration configuration) {
        FlinkKafkaConsumer<SessionEntity> kafkaConsumer = new FlinkKafkaConsumer<>(configuration.getString(KAFKA_CONSUMER_TOPIC),
                new TimestampDeserializationSchema(), getKafkaSourceProperty(configuration));

        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        kafkaConsumer.setStartFromGroupOffsets();

        return kafkaConsumer;
    }
   /* public static FlinkKafkaConsumer<String> getKafkaConsumerLists(List<String> topic) {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic,
                new SimpleStringSchema(), getKafkaSourceProperty(configuration));

        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        kafkaConsumer.setStartFromGroupOffsets();

        return kafkaConsumer;
    }*/
    public static SinkFunction<String> getKafkaSink(Configuration configuration) {
        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<String>(
                configuration.getString(KAFKA_PRODUCER_TOPIC),
                new SimpleStringSchema(),
                getKafkaSinkProperty(configuration),
                Optional.empty()
        );
        flinkKafkaProducer.setLogFailuresOnly(false);
        return flinkKafkaProducer;
    }


}
