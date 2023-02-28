package com.galaxy.tsg.config;

/**
 * Created by wk on 2021/1/6.
 */
public class commonConfig {


  public static final String KAFKA_CONSUMER_BROKER = CommonConfigurations.getStringProperty("kafka.consumer.broker");
  public static final String KAFKA_CONSUMER_GROUP_ID = CommonConfigurations.getStringProperty("kafka.consumer.group.id");
  public static final String KAFKA_CONSUMER_TOPIC = CommonConfigurations.getStringProperty("kafka.consumer.topic");
  public static final int    KAFKA_CONSUMER_PARALLELISM = CommonConfigurations.getIntProperty("kafka.consumer.parallelism");
  public static final String KAFKA_CONSUMER_SESSION_TIMEOUT_MS=CommonConfigurations.getStringProperty("kafka.consumer.session.timeout.ms");
  public static final String KAFKA_CONSUMER_MAX_POLL_RECORD=CommonConfigurations.getStringProperty("kafka.consumer.max.poll.records");
  public static final String KAFKA_CONSUMER_MAX_PARTITION_FETCH_BYTES=CommonConfigurations.getStringProperty("kafka.consumer.max.partition.fetch.bytes");
  public static final int KAFKA_CONSUMER_TOPURL_PARALLELISM=CommonConfigurations.getIntProperty("kafka.consumer.topurl.parallelism");




  public static final String KAFKA_PRODUCER_RETRIES = CommonConfigurations.getStringProperty("kafka.producer.retries");
  public static final String KAFKA_PRODUCER_LINGER_MS = CommonConfigurations.getStringProperty("kafka.producer.linger.ms");
  public static final String KAFKA_PRODUCER_REQUEST_TIMEOUT_MS = CommonConfigurations.getStringProperty("kafka.producer.request.timeout.ms");
  public static final String KAFKA_PRODUCER_BATCH_SIZE = CommonConfigurations.getStringProperty("kafka.producer.batch.size");
  public static final String KAFKA_PRODUCER_BUFFER_MEMORY = CommonConfigurations.getStringProperty("kafka.producer.buffer.memory");
  public static final String KAFKA_PRODUCER_MAX_REQUEST_SIZE = CommonConfigurations.getStringProperty("kafka.producer.max.request.size");
  public static final String KAFKA_PRODUCER_COMPRESSION_TYPE = CommonConfigurations.getStringProperty("kafka.producer.compression.type");



  public static final int TASK_PARALLELISM = CommonConfigurations.getIntProperty("task.parallelism");

  public static final int WATERMARK_TIME = CommonConfigurations.getIntProperty("watermark.time");
  public static final int WINDOW_TIME_MINUTE = CommonConfigurations.getIntProperty("window.time.minute");
  public static final int TOP_LIMIT = CommonConfigurations.getIntProperty("top.limit");
  public static final int URL_TOP_LIMIT = CommonConfigurations.getIntProperty("url.top.limit");

  public static final String KAFKA_USER = CommonConfigurations.getStringProperty("kafka.user");
  public static final String KAFKA_PIN = CommonConfigurations.getStringProperty("kafka.pin");
  public static final int KAFKA_SECURITY = CommonConfigurations.getIntProperty("kafka.security");
  public static final String TOOLS_LIBRARY = CommonConfigurations.getStringProperty("tools.library");


  public static final String KAFKA_PRODUCER_BROKER = CommonConfigurations.getStringProperty("kafka_producer_broker");

  public static final int TMP_TEST_TYPE = CommonConfigurations.getIntProperty("tmp.test.type");


}
