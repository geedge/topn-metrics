package com.galaxy.tsg;

import com.alibaba.fastjson2.JSON;
import com.galaxy.tsg.function.metricsAggregationReduce;
import com.galaxy.tsg.function.metricsCalculate;
import com.galaxy.tsg.function.topnHotItems;
import com.galaxy.tsg.pojo.resultEntity;
import com.galaxy.tsg.pojo.sessionEntity;
import com.galaxy.tsg.pojo.transformEntity;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static com.galaxy.tsg.config.commonConfig.*;
import static com.galaxy.tsg.util.kafkaUtils.getKafkaConsumer;
import static com.galaxy.tsg.util.kafkaUtils.getKafkaSink;

public class Toptask {
    private static final Logger LOG = LoggerFactory.getLogger(Toptask.class);

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<String> sourceForSession = env.addSource(getKafkaConsumer(KAFKA_CONSUMER_TOPIC)).setParallelism(KAFKA_CONSUMER_PARALLELISM);
        WatermarkStrategy<transformEntity> strategyForSession = WatermarkStrategy
                .<transformEntity>forBoundedOutOfOrderness(Duration.ofSeconds(WATERMARK_TIME))
                .withTimestampAssigner((transformEntity, timestamp) -> transformEntity.getTimestamp()*1000);


        SingleOutputStreamOperator<transformEntity> inputForSession = sourceForSession.map(new MapFunction<String, transformEntity>() {
            @Override
            public transformEntity map(String message) {
                transformEntity transformEntity = new transformEntity();
                try {
                    sessionEntity sessionEntity = JSON.parseObject(message, com.galaxy.tsg.pojo.sessionEntity.class);
                    transformEntity.setServer_ip(sessionEntity.getCommon_server_ip());
                    transformEntity.setClient_ip(sessionEntity.getCommon_client_ip());

                    transformEntity.setSubscriber_id(sessionEntity.getCommon_subscriber_id());
                    transformEntity.setFqdn(sessionEntity.getCommon_server_fqdn());
                    transformEntity.setExternal_ip(sessionEntity.getCommon_external_ip());
                    transformEntity.setInternal_ip(sessionEntity.getCommon_internal_ip());
                    transformEntity.setDomain(sessionEntity.getHttp_domain());
                    transformEntity.setDevice_group(sessionEntity.getCommon_device_group());
                    transformEntity.setDevice_id(sessionEntity.getCommon_device_id());
                    transformEntity.setData_center(sessionEntity.getCommon_data_center());
                    transformEntity.setVsys_id(sessionEntity.getCommon_vsys_id());
                    transformEntity.setTimestamp(sessionEntity.getCommon_recv_time());
                    transformEntity.setSessions(sessionEntity.getCommon_sessions());

                    transformEntity.setL4_protocol(sessionEntity.getCommon_l4_protocol());


                    if ((8L & sessionEntity.getCommon_flags()) == 8L) {

                        transformEntity.setOut_bytes(sessionEntity.getCommon_c2s_byte_num());
                        transformEntity.setOut_pkts(sessionEntity.getCommon_c2s_pkt_num());
                        transformEntity.setIn_bytes(sessionEntity.getCommon_s2c_byte_num());
                        transformEntity.setIn_pkts(sessionEntity.getCommon_s2c_pkt_num());

                    } else {
                        transformEntity.setOut_bytes(sessionEntity.getCommon_s2c_byte_num());
                        transformEntity.setOut_pkts(sessionEntity.getCommon_s2c_pkt_num());
                        transformEntity.setIn_bytes(sessionEntity.getCommon_c2s_byte_num());
                        transformEntity.setIn_pkts(sessionEntity.getCommon_c2s_pkt_num());


                    }


                } catch (Exception e) {
                    LOG.error("Entity Parsing ERROR");
                    transformEntity.setIfError(1);
                }
                return transformEntity;
            }
        }).filter(new FilterFunction<transformEntity>() {
            @Override
            public boolean filter(transformEntity entity) throws Exception {

                return entity.ifError != 1;
            }
        });


        //clientip聚合TOP

        SingleOutputStreamOperator<transformEntity> clientipdStream = inputForSession.filter(new FilterFunction<transformEntity>() {
            @Override
            public boolean filter(transformEntity value) throws Exception {
                return "IPv6_TCP".equals(value.getL4_protocol()) || "IPv4_TCP".equals(value.getL4_protocol());
            }
        }).assignTimestampsAndWatermarks(strategyForSession);

        SingleOutputStreamOperator<resultEntity> windowedStream = clientipdStream.keyBy(new groupBySelector("client_ip"))
                .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "client_ip")).setParallelism(TASK_PARALLELISM).name("client_ip");;
        DataStream<String> Stream = windowedStream.keyBy(new oneKeySelector())
                .process(new topnHotItems(TOP_LIMIT)).setParallelism(ORDERBY_PARALLELISM);
        Stream.addSink(getKafkaSink(KAFKA_PRODUCER_TOPIC)).setParallelism(SINK_PARALLELISM);


        //serverip聚合TOP


        SingleOutputStreamOperator<transformEntity> serveripdStream = inputForSession.filter(new FilterFunction<transformEntity>() {
            @Override
            public boolean filter(transformEntity value) throws Exception {
                return "IPv6_TCP".equals(value.getL4_protocol()) || "IPv4_TCP".equals(value.getL4_protocol());
            }
        }).assignTimestampsAndWatermarks(strategyForSession);

        SingleOutputStreamOperator<resultEntity> windowedStreamForServerIp = serveripdStream.keyBy(new groupBySelector("server_ip"))
                .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "server_ip")).setParallelism(TASK_PARALLELISM).name("server_ip");;
        DataStream<String> StreamForServerIp = windowedStreamForServerIp.keyBy(new oneKeySelector())
                .process(new topnHotItems(TOP_LIMIT)).setParallelism(ORDERBY_PARALLELISM);
        StreamForServerIp.addSink(getKafkaSink(KAFKA_PRODUCER_TOPIC)).setParallelism(SINK_PARALLELISM);


        //common_internal_ip聚合TOP
        SingleOutputStreamOperator<transformEntity> internalStream = inputForSession.filter(new FilterFunction<transformEntity>() {
            @Override
            public boolean filter(transformEntity value) throws Exception {
                return StringUtil.isNotEmpty(value.getInternal_ip());
            }
        }).assignTimestampsAndWatermarks(strategyForSession);

        SingleOutputStreamOperator<resultEntity> windowedStreamForInternal = internalStream.keyBy(new groupBySelector("internal_ip"))
                .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "internal_ip")).setParallelism(TASK_PARALLELISM).name("internal_ip");;
        DataStream<String> StreamForInternal = windowedStreamForInternal.keyBy(new oneKeySelector())
                .process(new topnHotItems(TOP_LIMIT)).setParallelism(ORDERBY_PARALLELISM);
        StreamForInternal.addSink(getKafkaSink(KAFKA_PRODUCER_TOPIC)).setParallelism(SINK_PARALLELISM);

        //common_external_ip聚合TOP

        SingleOutputStreamOperator<transformEntity> externalStream = inputForSession.filter(new FilterFunction<transformEntity>() {
            @Override
            public boolean filter(transformEntity value) throws Exception {
                return StringUtil.isNotEmpty(value.getExternal_ip());
            }
        }).assignTimestampsAndWatermarks(strategyForSession);

        SingleOutputStreamOperator<resultEntity> windowedStreamForExternal = externalStream.keyBy(new groupBySelector("external_ip"))
                .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "external_ip")).setParallelism(TASK_PARALLELISM).name("external_ip");;
        DataStream<String> StreamForExternal = windowedStreamForExternal.keyBy(new oneKeySelector())
                .process(new topnHotItems(TOP_LIMIT)).setParallelism(ORDERBY_PARALLELISM);
        StreamForExternal.addSink(getKafkaSink(KAFKA_PRODUCER_TOPIC)).setParallelism(SINK_PARALLELISM);

        //http_domain聚合TOP

        SingleOutputStreamOperator<transformEntity> domainStream = inputForSession.filter(new FilterFunction<transformEntity>() {
            @Override
            public boolean filter(transformEntity value) throws Exception {
                return StringUtil.isNotEmpty(value.getDomain());
            }
        }).assignTimestampsAndWatermarks(strategyForSession);

        SingleOutputStreamOperator<resultEntity> windowedStreamForDomain = domainStream.keyBy(new groupBySelector("server_domain"))
                .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "server_domain")).setParallelism(TASK_PARALLELISM).name("server_domain");;
        DataStream<String> StreamForDomain = windowedStreamForDomain.keyBy(new oneKeySelector())
                .process(new topnHotItems(TOP_LIMIT)).setParallelism(ORDERBY_PARALLELISM);
        StreamForDomain.addSink(getKafkaSink(KAFKA_PRODUCER_TOPIC)).setParallelism(SINK_PARALLELISM);

        SingleOutputStreamOperator<transformEntity> userStream = inputForSession.filter(new FilterFunction<transformEntity>() {
            @Override
            public boolean filter(transformEntity value) throws Exception {
                return StringUtil.isNotEmpty(value.getSubscriber_id());
            }
        }).assignTimestampsAndWatermarks(strategyForSession);

        //common_subscriber_id聚合TOP
        SingleOutputStreamOperator<resultEntity> windowedStreamForUser = userStream.keyBy(new groupBySelector("subscriber_id"))
                .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "subscriber_id")).setParallelism(TASK_PARALLELISM).name("subscriber_id");;
        DataStream<String> StreamForUser = windowedStreamForUser.keyBy(new oneKeySelector())
                .process(new topnHotItems(TOP_LIMIT)).setParallelism(ORDERBY_PARALLELISM);
        StreamForUser.addSink(getKafkaSink(KAFKA_PRODUCER_TOPIC)).setParallelism(SINK_PARALLELISM);

        SingleOutputStreamOperator<transformEntity> fqdnStream = inputForSession.filter(new FilterFunction<transformEntity>() {
            @Override
            public boolean filter(transformEntity value) throws Exception {
                return StringUtil.isNotEmpty(value.getFqdn());
            }
        }).assignTimestampsAndWatermarks(strategyForSession);

        SingleOutputStreamOperator<resultEntity> windowedStreamForFqdn = fqdnStream.keyBy(new groupBySelector("server_fqdn"))
                .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "server_fqdn")).setParallelism(TASK_PARALLELISM).name("server_fqdn");
        DataStream<String> StreamForFqdn = windowedStreamForFqdn.keyBy(new oneKeySelector())
                .process(new topnHotItems(TOP_LIMIT)).setParallelism(ORDERBY_PARALLELISM);
        StreamForFqdn.addSink(getKafkaSink(KAFKA_PRODUCER_TOPIC)).setParallelism(SINK_PARALLELISM);

        env.execute(JOB_NAME);

    }


    public static class groupBySelector implements KeySelector<transformEntity, Tuple5<String, Long, String, String, String>> {

        public String key;

        public groupBySelector(String key) {
            this.key = key;
        }

        @Override
        public Tuple5<String, Long, String, String, String> getKey(transformEntity transformEntity) throws Exception {

            Tuple5<String, Long, String, String, String> tuple = null;
            transformEntity.setKey_by(key);
            switch (key) {
                case "client_ip":
                    tuple = new Tuple5<>(transformEntity.getClient_ip(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                    break;
                case "server_ip":
                    tuple = new Tuple5<>(transformEntity.getServer_ip(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                    break;
                case "internal_ip":
                    tuple = new Tuple5<>(transformEntity.getInternal_ip(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                    break;
                case "external_ip":
                    tuple = new Tuple5<>(transformEntity.getExternal_ip(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                    break;
                case "server_domain":
                    tuple = new Tuple5<>(transformEntity.getDomain(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                    break;

                case "subscriber_id":
                    tuple = new Tuple5<>(transformEntity.getSubscriber_id(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                    break;
                case "server_fqdn":
                    tuple = new Tuple5<>(transformEntity.getFqdn(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                    break;

                default:

            }
            return tuple;
        }
    }


    public static class oneKeySelector implements KeySelector<resultEntity, Tuple1<String>> {

        @Override
        public Tuple1<String> getKey(resultEntity entity) throws Exception {
            return new Tuple1<>(entity.getOrder_by());
        }
    }


}