package com.galaxy.tsg.config;

/**
 * Created by wk on 2021/1/6.
 */
public class commonConfig {


  public static final String KAFKA_CONSUMER_BROKER = commonConfigurations.getStringProperty("kafka.consumer.broker");
  public static final String KAFKA_CONSUMER_GROUP_ID = commonConfigurations.getStringProperty("kafka.consumer.group.id");
  public static final String KAFKA_CONSUMER_TOPIC = commonConfigurations.getStringProperty("kafka.consumer.topic");
  public static final int    KAFKA_CONSUMER_PARALLELISM = commonConfigurations.getIntProperty("kafka.consumer.parallelism");
  public static final String KAFKA_CONSUMER_SESSION_TIMEOUT_MS= commonConfigurations.getStringProperty("kafka.consumer.session.timeout.ms");
  public static final String KAFKA_CONSUMER_MAX_POLL_RECORD= commonConfigurations.getStringProperty("kafka.consumer.max.poll.records");
  public static final String KAFKA_CONSUMER_MAX_PARTITION_FETCH_BYTES= commonConfigurations.getStringProperty("kafka.consumer.max.partition.fetch.bytes");



  public static final String KAFKA_PRODUCER_TOPIC = commonConfigurations.getStringProperty("kafka.producer.topic");
  public static final String KAFKA_PRODUCER_RETRIES = commonConfigurations.getStringProperty("kafka.producer.retries");
  public static final String KAFKA_PRODUCER_LINGER_MS = commonConfigurations.getStringProperty("kafka.producer.linger.ms");
  public static final String KAFKA_PRODUCER_REQUEST_TIMEOUT_MS = commonConfigurations.getStringProperty("kafka.producer.request.timeout.ms");
  public static final String KAFKA_PRODUCER_BATCH_SIZE = commonConfigurations.getStringProperty("kafka.producer.batch.size");
  public static final String KAFKA_PRODUCER_BUFFER_MEMORY = commonConfigurations.getStringProperty("kafka.producer.buffer.memory");
  public static final String KAFKA_PRODUCER_MAX_REQUEST_SIZE = commonConfigurations.getStringProperty("kafka.producer.max.request.size");
  public static final String KAFKA_PRODUCER_COMPRESSION_TYPE = commonConfigurations.getStringProperty("kafka.producer.compression.type");
  public static final String KAFKA_PRODUCER_BROKER = commonConfigurations.getStringProperty("kafka_producer_broker");



  public static final String JOB_NAME = commonConfigurations.getStringProperty("job.name");
  public static final int TASK_PARALLELISM = commonConfigurations.getIntProperty("task.parallelism");
  public static final int ORDERBY_PARALLELISM = commonConfigurations.getIntProperty("orderby.parallelism");
  public static final int SINK_PARALLELISM = commonConfigurations.getIntProperty("sink.parallelism");

  public static final int WATERMARK_TIME = commonConfigurations.getIntProperty("watermark.time");
  public static final int WINDOW_TIME_MINUTE = commonConfigurations.getIntProperty("window.time.minute");
  public static final int TOP_LIMIT = commonConfigurations.getIntProperty("top.limit");

  public static final String KAFKA_CONSUMER_USER = commonConfigurations.getStringProperty("kafka.consumer.user");
  public static final String KAFKA_CONSUMER_PIN = commonConfigurations.getStringProperty("kafka.consumer.pin");
  public static final int KAFKA_CONSUMER_SECURITY = commonConfigurations.getIntProperty("kafka.consumer.security");
  public static final String TOOLS_CONSUMER_LIBRARY = commonConfigurations.getStringProperty("tools.consumer.library");

  public static final String KAFKA_PRODUCER_USER = commonConfigurations.getStringProperty("kafka.producer.user");
  public static final String KAFKA_PRODUCER_PIN = commonConfigurations.getStringProperty("kafka.producer.pin");
  public static final int KAFKA_PRODUCER_SECURITY = commonConfigurations.getIntProperty("kafka.producer.security");
  public static final String TOOLS_PRODUCER_LIBRARY = commonConfigurations.getStringProperty("tools.producer.library");



}
