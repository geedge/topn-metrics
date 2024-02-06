package com.galaxy.tsg.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Created by wk on 2021/1/6.
 */
public class CommonConfig {

  public static final ConfigOption<String> KAFKA_CONSUMER_BROKER = ConfigOptions.key("kafka.consumer.broker").stringType().defaultValue("");
  public static final ConfigOption<String> KAFKA_CONSUMER_GROUP_ID = ConfigOptions.key("kafka.consumer.group.id").stringType().defaultValue("");
  public static final ConfigOption<String> KAFKA_CONSUMER_TOPIC = ConfigOptions.key("kafka.consumer.topic").stringType().defaultValue("");
  public static final ConfigOption<Integer> KAFKA_CONSUMER_PARALLELISM = ConfigOptions.key("kafka.consumer.parallelism").intType().defaultValue(0);
  public static final ConfigOption<String> KAFKA_CONSUMER_SESSION_TIMEOUT_MS= ConfigOptions.key("kafka.consumer.session.timeout.ms").stringType().defaultValue("60000");
  public static final ConfigOption<String> KAFKA_CONSUMER_MAX_POLL_RECORD= ConfigOptions.key("kafka.consumer.max.poll.records").stringType().defaultValue("3000");
  public static final ConfigOption<String> KAFKA_CONSUMER_MAX_PARTITION_FETCH_BYTES= ConfigOptions.key("kafka.consumer.max.partition.fetch.bytes").stringType().defaultValue("31457280");



  public static final ConfigOption<String> KAFKA_PRODUCER_TOPIC = ConfigOptions.key("kafka.producer.topic").stringType().defaultValue("");
  public static final ConfigOption<String> KAFKA_PRODUCER_RETRIES = ConfigOptions.key("kafka.producer.retries").stringType().defaultValue("1");
  public static final ConfigOption<String> KAFKA_PRODUCER_LINGER_MS = ConfigOptions.key("kafka.producer.linger.ms").stringType().defaultValue("10");
  public static final ConfigOption<String> KAFKA_PRODUCER_REQUEST_TIMEOUT_MS = ConfigOptions.key("kafka.producer.request.timeout.ms").stringType().defaultValue("30000");
  public static final ConfigOption<String> KAFKA_PRODUCER_BATCH_SIZE = ConfigOptions.key("kafka.producer.batch.size").stringType().defaultValue("262144");
  public static final ConfigOption<String> KAFKA_PRODUCER_BUFFER_MEMORY = ConfigOptions.key("kafka.producer.buffer.memory").stringType().defaultValue("134217728");
  public static final ConfigOption<String> KAFKA_PRODUCER_MAX_REQUEST_SIZE = ConfigOptions.key("kafka.producer.max.request.size").stringType().defaultValue("10485760");
  public static final ConfigOption<String> KAFKA_PRODUCER_COMPRESSION_TYPE = ConfigOptions.key("kafka.producer.compression.type").stringType().defaultValue("none");
  public static final ConfigOption<String> KAFKA_PRODUCER_BROKER = ConfigOptions.key("kafka.producer.broker").stringType().defaultValue("");


  public static final ConfigOption<String> JOB_NAME = ConfigOptions.key("job.name").stringType().defaultValue("");
  public static final ConfigOption<Integer> TASK_PARALLELISM = ConfigOptions.key("task.parallelism").intType().defaultValue(0);
  public static final ConfigOption<Integer> ORDERBY_PARALLELISM = ConfigOptions.key("orderby.parallelism").intType().defaultValue(0);
  public static final ConfigOption<Integer> SINK_PARALLELISM = ConfigOptions.key("sink.parallelism").intType().defaultValue(0);

  public static final ConfigOption<Integer> WATERMARK_TIME = ConfigOptions.key("watermark.time").intType().defaultValue(0);
  public static final ConfigOption<Integer> WINDOW_TIME_MINUTE = ConfigOptions.key("window.time.minute").intType().defaultValue(0);
  public static final ConfigOption<Integer> TOP_LIMIT = ConfigOptions.key("top.limit").intType().defaultValue(0);

  public static final ConfigOption<String> KAFKA_CONSUMER_USER = ConfigOptions.key("kafka.consumer.user").stringType().defaultValue("");
  public static final ConfigOption<String> KAFKA_CONSUMER_PIN = ConfigOptions.key("kafka.consumer.pin").stringType().defaultValue("");
  public static final ConfigOption<Integer> KAFKA_CONSUMER_SECURITY = ConfigOptions.key("kafka.consumer.security").intType().defaultValue(0);
  public static final ConfigOption<String> TOOLS_CONSUMER_LIBRARY = ConfigOptions.key("tools.consumer.library").stringType().defaultValue("");

  public static final ConfigOption<String> KAFKA_PRODUCER_USER = ConfigOptions.key("kafka.producer.user").stringType().defaultValue("");
  public static final ConfigOption<String> KAFKA_PRODUCER_PIN = ConfigOptions.key("kafka.producer.pin").stringType().defaultValue("");
  public static final ConfigOption<Integer> KAFKA_PRODUCER_SECURITY = ConfigOptions.key("kafka.producer.security").intType().defaultValue(0);
  public static final ConfigOption<String> TOOLS_PRODUCER_LIBRARY = ConfigOptions.key("tools.producer.library").stringType().defaultValue("");



}
