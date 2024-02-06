package com.galaxy.tsg;

import com.galaxy.tsg.config.CommonConfig;
import com.galaxy.tsg.function.*;
import com.galaxy.tsg.pojo.ResultEntity;
import com.galaxy.tsg.pojo.SessionEntity;
import com.galaxy.tsg.pojo.TransformEntity;
import com.galaxy.tsg.selector.GroupBySelector;
import com.galaxy.tsg.selector.OrderBySelector;
import com.geedgenetworks.utils.StringUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;

import static com.galaxy.tsg.config.CommonConfig.*;
import static com.galaxy.tsg.util.KafkaUtils.getKafkaConsumer;
import static com.galaxy.tsg.util.KafkaUtils.getKafkaSink;

public class Toptask {
    private static final Logger LOG = LoggerFactory.getLogger(Toptask.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool serviceConfig = ParameterTool.fromPropertiesFile(args[0]);
        Configuration configurationService = serviceConfig.getConfiguration();
        // global register
        env.getConfig().setGlobalJobParameters(configurationService);

        WatermarkStrategy<SessionEntity> strategyForSession = WatermarkStrategy
                .<SessionEntity>forBoundedOutOfOrderness(Duration.ofSeconds(configurationService.get(CommonConfig.WATERMARK_TIME)))
                .withTimestampAssigner((sessionEntity, timestamp) -> sessionEntity.getRecv_time());

        DataStream<SessionEntity> sourceForSession = env.addSource(getKafkaConsumer(configurationService)).setParallelism(configurationService.get(CommonConfig.KAFKA_CONSUMER_PARALLELISM)).assignTimestampsAndWatermarks(strategyForSession);;
        SingleOutputStreamOperator<TransformEntity> inputForSession = sourceForSession.flatMap(new FlatMapFunction()
         ).filter(new FilterFunction<TransformEntity>() {
            @Override
            public boolean filter(TransformEntity entity) throws Exception {

                return entity.getIfError() != 1;
            }
        });

        buildJobTuple(configurationService,inputForSession,"client_ip");
        buildJobTuple(configurationService,inputForSession,"server_ip");
        buildJobTuple(configurationService,inputForSession,"internal_ip");
        buildJobTuple(configurationService,inputForSession,"external_ip");
        buildJobTuple(configurationService,inputForSession,"server_domain");
        buildJobTuple(configurationService,inputForSession,"subscriber_id");
        buildJobTuple(configurationService,inputForSession,"server_fqdn");
        env.execute(configurationService.getString(JOB_NAME));

    }



    private static void buildJobTuple(Configuration configurationService, SingleOutputStreamOperator<TransformEntity> inputForSession, String key) {

        SingleOutputStreamOperator<TransformEntity> event = null;
        switch (key) {
            case "client_ip":
                event= inputForSession.filter(new FilterFunction<TransformEntity>() {
                    @Override
                    public boolean filter(TransformEntity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getClient_ip());
                    }
                });
                break;
            case "server_ip":
                event= inputForSession.filter(new FilterFunction<TransformEntity>() {
                    @Override
                    public boolean filter(TransformEntity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getServer_ip());
                    }
                });
                break;
            case "internal_ip":
                event= inputForSession.filter(new FilterFunction<TransformEntity>() {
                    @Override
                    public boolean filter(TransformEntity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getInternal_ip());
                    }
                });
                break;
            case "external_ip":
                event= inputForSession.filter(new FilterFunction<TransformEntity>() {
                    @Override
                    public boolean filter(TransformEntity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getExternal_ip());
                    }
                });
                break;
            case "server_domain":
                event= inputForSession.filter(new FilterFunction<TransformEntity>() {
                    @Override
                    public boolean filter(TransformEntity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getDomain());
                    }
                });
                break;
            case "subscriber_id":
                event= inputForSession.filter(new FilterFunction<TransformEntity>() {
                    @Override
                    public boolean filter(TransformEntity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getSubscriber_id());
                    }
                });
                break;
            case "server_fqdn":
                event= inputForSession.filter(new FilterFunction<TransformEntity>() {
                    @Override
                    public boolean filter(TransformEntity value) throws Exception {
                        return StringUtil.isNotEmpty(value.getFqdn());
                    }
                });
                break;

            default:
                break;
        }
                SingleOutputStreamOperator<ResultEntity> windowedStreamForFqdn = Objects.requireNonNull(event).keyBy(new GroupBySelector(key))
                        .window(TumblingEventTimeWindows.of(Time.minutes(configurationService.getInteger(CommonConfig.WINDOW_TIME_MINUTE))))
                        .reduce(new MetricsAggregationReduce(), new MetricsCalculate()).setParallelism(configurationService.getInteger(CommonConfig.TASK_PARALLELISM)).name(key);
                DataStream<String> StreamForKey = windowedStreamForFqdn.keyBy(new OrderBySelector())
                        .process(new TopnHotItems(configurationService.getInteger(CommonConfig.TOP_LIMIT))).setParallelism(configurationService.getInteger(CommonConfig.ORDERBY_PARALLELISM));
                StreamForKey.addSink(getKafkaSink(configurationService)).setParallelism(configurationService.getInteger(CommonConfig.SINK_PARALLELISM));


        }




}