package com.galaxy.tsg;

import com.alibaba.fastjson.JSON;
import com.galaxy.tsg.function.*;
import com.galaxy.tsg.pojo.Entity;
import com.galaxy.tsg.pojo.ResultEntity;
import com.galaxy.tsg.pojo.UrlEntity;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

import static com.galaxy.tsg.config.commonConfig.*;
import static com.galaxy.tsg.util.KafkaUtils.*;

public class Toptask {
    private static final Logger LOG = LoggerFactory.getLogger(Toptask.class);

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定使用事件时间
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStream<String> sourceForSession = env.addSource(getKafkaConsumer("SESSION-RECORD-COMPLETED")).setParallelism(KAFKA_CONSUMER_PARALLELISM);
        WatermarkStrategy<Entity> strategyForSession = WatermarkStrategy
                .<Entity>forBoundedOutOfOrderness(Duration.ofSeconds(WATERMARK_TIME))
                .withTimestampAssigner((Entity, timestamp) -> Entity.getCommon_recv_time() * 1000);

        List<String> topics = new LinkedList<>();
        topics.add("SECURITY-EVENT-COMPLETED");
        topics.add("PROXY-EVENT-COMPLETED");
        DataStream<String> sourceForUrl = env.addSource(getKafkaConsumerLists(topics)).setParallelism(KAFKA_CONSUMER_TOPURL_PARALLELISM);
        WatermarkStrategy<UrlEntity> strategyForSecurity = WatermarkStrategy
                .<UrlEntity>forBoundedOutOfOrderness(Duration.ofSeconds(WATERMARK_TIME))
                .withTimestampAssigner((UrlEntity, timestamp) -> UrlEntity.getCommon_recv_time() * 1000);


        SingleOutputStreamOperator<Entity> inputForSession = sourceForSession.map(new MapFunction<String, Entity>() {
            @Override
            public Entity map(String message) {
                Entity entity = new Entity();
                try {
                    entity = JSON.parseObject(message, Entity.class);

                } catch (Exception e) {
                    LOG.error("Entity Parsing ERROR");
                    entity.setIfError(1);
                }
                return entity;
            }
        }).filter(new FilterFunction<Entity>() {
            @Override
            public boolean filter(Entity entity) throws Exception {

                return entity.ifError != 1;
            }
        });

        SingleOutputStreamOperator<UrlEntity> inputForUrl = sourceForUrl.map(new MapFunction<String, UrlEntity>() {
            @Override
            public UrlEntity map(String message) {
                UrlEntity entity = new UrlEntity();
                try {
                    entity = JSON.parseObject(message, UrlEntity.class);

                } catch (Exception e) {
                    LOG.error("Entity Parsing ERROR");
                    entity.setIfError(1);
                }
                return entity;
            }
        }).filter(new FilterFunction<UrlEntity>() {
            @Override
            public boolean filter(UrlEntity entity) throws Exception {

                return entity.ifError != 1;
            }
        });

        switch (TMP_TEST_TYPE) {
            case 1:

                //clientip聚合TOP

                SingleOutputStreamOperator<Entity> clientipdStream = inputForSession.filter(new FilterFunction<Entity>() {
                    @Override
                    public boolean filter(Entity value) throws Exception {
                        return "IPv6_TCP".equals(value.getCommon_l4_protocol()) || "IPv4_TCP".equals(value.getCommon_l4_protocol());
                    }
                }).assignTimestampsAndWatermarks(strategyForSession);

                SingleOutputStreamOperator<ResultEntity> windowedStream = clientipdStream.keyBy(new groupBySelector("common_client_ip"))
                        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                        .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "common_client_ip")).setParallelism(TASK_PARALLELISM);
                DataStream<String> windoweddStream = windowedStream.keyBy(new oneKeySelector())
                        .process(new TopNHotItems(TOP_LIMIT)).setParallelism(3);
                windoweddStream.addSink(getKafkaSink("TOP-CLIENT-IP")).setParallelism(3);

                //serverip聚合TOP

                SingleOutputStreamOperator<Entity> serveripdStream = inputForSession.filter(new FilterFunction<Entity>() {
                    @Override
                    public boolean filter(Entity value) throws Exception {
                        return "IPv6_TCP".equals(value.getCommon_l4_protocol()) || "IPv4_TCP".equals(value.getCommon_l4_protocol());
                    }
                }).assignTimestampsAndWatermarks(strategyForSession);

                SingleOutputStreamOperator<ResultEntity> windowedStreamForServerIp = serveripdStream.keyBy(new groupBySelector("common_server_ip"))
                        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                        .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "common_server_ip")).setParallelism(TASK_PARALLELISM);
                DataStream<String> windoweddStreamForServerIp = windowedStreamForServerIp.keyBy(new oneKeySelector())
                        .process(new TopNHotItems(TOP_LIMIT)).setParallelism(3);
                windoweddStreamForServerIp.addSink(getKafkaSink("TOP-SERVER-IP")).setParallelism(3);


                //common_internal_ip聚合TOP
                SingleOutputStreamOperator<Entity> internalStream = inputForSession.filter(new FilterFunction<Entity>() {
                    @Override
                    public boolean filter(Entity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getCommon_internal_ip());
                    }
                }).assignTimestampsAndWatermarks(strategyForSession);

                SingleOutputStreamOperator<ResultEntity> windowedStreamForInternal = internalStream.keyBy(new groupBySelector("common_internal_ip"))
                        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                        .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "common_internal_ip")).setParallelism(TASK_PARALLELISM);
                DataStream<String> WindoweddStreamForInternal = windowedStreamForInternal.keyBy(new oneKeySelector())
                        .process(new TopNHotItems(TOP_LIMIT)).setParallelism(3);
                WindoweddStreamForInternal.addSink(getKafkaSink("TOP-INTERNAL-HOST")).setParallelism(3);

                //common_external_ip聚合TOP

                SingleOutputStreamOperator<Entity> externalStream = inputForSession.filter(new FilterFunction<Entity>() {
                    @Override
                    public boolean filter(Entity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getCommon_external_ip());
                    }
                }).assignTimestampsAndWatermarks(strategyForSession);

                SingleOutputStreamOperator<ResultEntity> windowedStreamForExternal = externalStream.keyBy(new groupBySelector("common_external_ip"))
                        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                        .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "common_external_ip")).setParallelism(TASK_PARALLELISM);
                DataStream<String> WindoweddStreamForExternal = windowedStreamForExternal.keyBy(new oneKeySelector())
                        .process(new TopNHotItems(TOP_LIMIT)).setParallelism(3);
                WindoweddStreamForExternal.addSink(getKafkaSink("TOP-EXTERNAL-HOST")).setParallelism(3);

                //http_domain聚合TOP

                SingleOutputStreamOperator<Entity> domainStream = inputForSession.filter(new FilterFunction<Entity>() {
                    @Override
                    public boolean filter(Entity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getHttp_domain());
                    }
                }).assignTimestampsAndWatermarks(strategyForSession);

                SingleOutputStreamOperator<ResultEntity> windowedStreamForDomain = domainStream.keyBy(new groupBySelector("http_domain"))
                        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                        .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "http_domain")).setParallelism(TASK_PARALLELISM);
                DataStream<String> WindoweddStreamForDomain = windowedStreamForDomain.keyBy(new oneKeySelector())
                        .process(new TopNHotItems(TOP_LIMIT)).setParallelism(3);
                WindoweddStreamForDomain.addSink(getKafkaSink("TOP-WEBSITE-DOMAIN")).setParallelism(3);

                SingleOutputStreamOperator<Entity> userStream = inputForSession.filter(new FilterFunction<Entity>() {
                    @Override
                    public boolean filter(Entity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getCommon_subscriber_id());
                    }
                }).assignTimestampsAndWatermarks(strategyForSession);

                //common_subscriber_id聚合TOP
                SingleOutputStreamOperator<ResultEntity> windowedStreamForUser = userStream.keyBy(new groupBySelector("common_subscriber_id"))
                        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                        .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "common_subscriber_id")).setParallelism(TASK_PARALLELISM);
                DataStream<String> WindoweddStreamForUser = windowedStreamForUser.keyBy(new oneKeySelector())
                        .process(new TopNHotItems(TOP_LIMIT)).setParallelism(3);
                WindoweddStreamForUser.addSink(getKafkaSink("TOP-USER")).setParallelism(3);

                SingleOutputStreamOperator<Entity> appNameStream = inputForSession.filter(new FilterFunction<Entity>() {
                    @Override
                    public boolean filter(Entity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getCommon_app_label());
                    }
                }).assignTimestampsAndWatermarks(strategyForSession);


                //common_app_label聚合求全量
                appNameStream.keyBy(new groupBySelector("common_app_label"))
                        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                        .reduce(new metricsAggregationReduce(), new metricsCalculateForApp()).addSink(getKafkaSink("TRAFFIC-APP-STAT")).setParallelism(TASK_PARALLELISM);


                SingleOutputStreamOperator<UrlEntity> UrlStream = inputForUrl.filter(new FilterFunction<UrlEntity>() {
                    @Override
                    public boolean filter(UrlEntity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getHttp_url());
                    }
                }).assignTimestampsAndWatermarks(strategyForSecurity);

                //url聚合session求top
                SingleOutputStreamOperator<ResultEntity> windowedStreamForUrl = UrlStream.keyBy(new twoKeySelector())
                        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
                        .reduce(new UrlAggregationReduce(), new metricsCalculateForUrl(URL_TOP_LIMIT)).setParallelism(TASK_PARALLELISM);
                DataStream<String> WindoweddStreamForUrl = windowedStreamForUrl.keyBy(new oneKeySelector())
                        .process(new TopNHotItemsForUrl(URL_TOP_LIMIT)).setParallelism(1);
                WindoweddStreamForUrl.addSink(getKafkaSink("TOP-URLS")).setParallelism(3);

                break;
            case 2:
                //datasketch


                //Session_record top1000  21个窗口一并计算
                SingleOutputStreamOperator<Entity> clientipdStream2 = inputForSession.filter(new FilterFunction<Entity>() {
                    @Override
                    public boolean filter(Entity value) throws Exception {
                        return "IPv6_TCP".equals(value.getCommon_l4_protocol()) || "IPv4_TCP".equals(value.getCommon_l4_protocol());
                    }
                }).assignTimestampsAndWatermarks(strategyForSession);


                AllWindowedStream<Entity, TimeWindow> entityTimeWindowAllWindowedStream = clientipdStream2.windowAll(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)));
                SingleOutputStreamOperator<String> aggregate = entityTimeWindowAllWindowedStream.aggregate(new UserHashMapCountAgg5(), new UserCountWindowResult5());
                aggregate.print();






                //Security_record top 1000    1个窗口、proxy_record top 1000     1个窗口
                SingleOutputStreamOperator<UrlEntity> UrlStream2 = inputForUrl.filter(new FilterFunction<UrlEntity>() {
                    @Override
                    public boolean filter(UrlEntity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getHttp_url());
                    }
                }).assignTimestampsAndWatermarks(strategyForSecurity);


                AllWindowedStream<UrlEntity, TimeWindow> urlEntityTimeWindowAllWindowedStream = UrlStream2.windowAll(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)));
                SingleOutputStreamOperator<String> aggregate1 = urlEntityTimeWindowAllWindowedStream.aggregate(new UserHashMapCountAgg6(), new UserCountWindowResult5());
                aggregate1.print();



                //clientip聚合TOP

//                SingleOutputStreamOperator<Entity> clientipdStream3 = inputForSession.filter(new FilterFunction<Entity>() {
//                    @Override
//                    public boolean filter(Entity value) throws Exception {
//                        return "IPv6_TCP".equals(value.getCommon_l4_protocol()) || "IPv4_TCP".equals(value.getCommon_l4_protocol());
//                    }
//                }).assignTimestampsAndWatermarks(strategyForSession);
//
//                SingleOutputStreamOperator<ResultEntity> windowedStream3 = clientipdStream3.keyBy(new groupBySelector("common_client_ip"))
//                        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_TIME_MINUTE)))
//                        .reduce(new metricsAggregationReduce(), new metricsCalculate(TOP_LIMIT, "common_client_ip")).setParallelism(TASK_PARALLELISM);
//                DataStream<String> windoweddStream3 = windowedStream3.keyBy(new oneKeySelector())
//                        .process(new TopNHotItems(TOP_LIMIT)).setParallelism(3);
//                windoweddStream3.print();






                break;

        }


        env.execute("TOP-task");

    }


    public static class groupBySelector implements KeySelector<Entity, Tuple4<String, Long, String, String>> {

        public String key;

        public groupBySelector(String key) {
            this.key = key;
        }

        @Override
        public Tuple4<String, Long, String, String> getKey(Entity entity) throws Exception {

            Tuple4<String, Long, String, String> tuple = null;
            switch (key) {
                case "common_client_ip":
                    tuple = new Tuple4<>(entity.getCommon_client_ip(), entity.getCommon_vsys_id(), entity.getCommon_device_group(), entity.getCommon_data_center());
                    break;
                case "common_server_ip":
                    tuple = new Tuple4<>(entity.getCommon_server_ip(), entity.getCommon_vsys_id(), entity.getCommon_device_group(), entity.getCommon_data_center());
                    break;
                case "common_internal_ip":
                    tuple = new Tuple4<>(entity.getCommon_internal_ip(), entity.getCommon_vsys_id(), entity.getCommon_device_group(), entity.getCommon_data_center());
                    break;
                case "common_external_ip":
                    tuple = new Tuple4<>(entity.getCommon_external_ip(), entity.getCommon_vsys_id(), entity.getCommon_device_group(), entity.getCommon_data_center());
                    break;
                case "http_domain":
                    tuple = new Tuple4<>(entity.getHttp_domain(), entity.getCommon_vsys_id(), entity.getCommon_device_group(), entity.getCommon_data_center());
                    break;

                case "common_subscriber_id":
                    tuple = new Tuple4<>(entity.getCommon_subscriber_id(), entity.getCommon_vsys_id(), entity.getCommon_device_group(), entity.getCommon_data_center());
                    break;
                case "common_app_label":
                    tuple = new Tuple4<>(entity.getCommon_app_label(), entity.getCommon_vsys_id(), entity.getCommon_device_group(), entity.getCommon_data_center());
                    break;

                default:

            }
            return tuple;
        }
    }


    public static class oneKeySelector implements KeySelector<ResultEntity, Tuple1<String>> {

        @Override
        public Tuple1<String> getKey(ResultEntity entity) throws Exception {
            return new Tuple1<>(entity.getOrder_by());
        }
    }

    public static class twoKeySelector implements KeySelector<UrlEntity, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> getKey(UrlEntity entity) throws Exception {
            return new Tuple2<>(entity.getHttp_url(), entity.getCommon_vsys_id());
        }
    }
}