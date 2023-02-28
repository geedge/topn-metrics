package com.galaxy.tsg.util;

import com.galaxy.tsg.config.commonConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.config.SslConfigs;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class KafkaUtils {


    public static Properties getKafkaSourceProperty() {
        Properties properties = new Properties();
        properties.setProperty("group.id", commonConfig.KAFKA_CONSUMER_GROUP_ID);
        properties.setProperty("bootstrap.servers", commonConfig.KAFKA_CONSUMER_BROKER);
        properties.setProperty("session.timeout.ms", commonConfig.KAFKA_CONSUMER_SESSION_TIMEOUT_MS);
        properties.setProperty("max.poll.records", commonConfig.KAFKA_CONSUMER_MAX_POLL_RECORD);
        properties.setProperty("max.partition.fetch.bytes", commonConfig.KAFKA_CONSUMER_MAX_PARTITION_FETCH_BYTES);

        switch (commonConfig.KAFKA_SECURITY) {
            case 1:
                properties.put("security.protocol", "SSL");
                properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
                properties.put("ssl.keystore.location", commonConfig.TOOLS_LIBRARY + "keystore.jks");
                properties.put("ssl.keystore.password", commonConfig.KAFKA_PIN);
                properties.put("ssl.truststore.location", commonConfig.TOOLS_LIBRARY + "truststore.jks");
                properties.put("ssl.truststore.password", commonConfig.KAFKA_PIN);
                properties.put("ssl.key.password", commonConfig.KAFKA_PIN);
                break;
            case 2:
                properties.put("security.protocol", "SASL_PLAINTEXT");
                properties.put("sasl.mechanism", "PLAIN");
                properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username="
                        + commonConfig.KAFKA_USER + " password=" + commonConfig.KAFKA_PIN + ";");
                break;
            default:
        }

        return properties;
    }

    private static Properties getKafkaSinkProperty() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", commonConfig.KAFKA_PRODUCER_BROKER);

        properties.put("acks", "1");
        properties.put("retries", commonConfig.KAFKA_PRODUCER_RETRIES);
        properties.put("linger.ms", commonConfig.KAFKA_PRODUCER_LINGER_MS);
        properties.put("request.timeout.ms", commonConfig.KAFKA_PRODUCER_REQUEST_TIMEOUT_MS);
        properties.put("batch.size", commonConfig.KAFKA_PRODUCER_BATCH_SIZE);
        properties.put("buffer.memory", commonConfig.KAFKA_PRODUCER_BUFFER_MEMORY);
        properties.put("max.request.size", commonConfig.KAFKA_PRODUCER_MAX_REQUEST_SIZE);
        properties.put("compression.type", commonConfig.KAFKA_PRODUCER_COMPRESSION_TYPE);

        switch (commonConfig.KAFKA_SECURITY) {
            case 1:
                properties.put("security.protocol", "SSL");
                properties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
                properties.put("ssl.keystore.location", commonConfig.TOOLS_LIBRARY + "keystore.jks");
                properties.put("ssl.keystore.password", commonConfig.KAFKA_PIN);
                properties.put("ssl.truststore.location", commonConfig.TOOLS_LIBRARY + "truststore.jks");
                properties.put("ssl.truststore.password", commonConfig.KAFKA_PIN);
                properties.put("ssl.key.password", commonConfig.KAFKA_PIN);
                break;
            case 2:
                properties.put("security.protocol", "SASL_PLAINTEXT");
                properties.put("sasl.mechanism", "PLAIN");
                properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username="
                        + commonConfig.KAFKA_USER + " password=" + commonConfig.KAFKA_PIN + ";");
                break;
            default:
        }
        return properties;
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic) {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic,
                new SimpleStringSchema(), getKafkaSourceProperty());

        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        kafkaConsumer.setStartFromGroupOffsets();

        return kafkaConsumer;
    }
    public static FlinkKafkaConsumer<String> getKafkaConsumerLists(List<String> topic) {
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic,
                new SimpleStringSchema(), getKafkaSourceProperty());

        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        kafkaConsumer.setStartFromGroupOffsets();

        return kafkaConsumer;
    }
    public static SinkFunction<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(
                topic,
                new SimpleStringSchema(),
                getKafkaSinkProperty(),
                Optional.empty()
        );
    }


}
