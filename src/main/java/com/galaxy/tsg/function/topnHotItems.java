package com.galaxy.tsg.function;

import com.alibaba.fastjson2.JSONObject;
import com.galaxy.tsg.pojo.*;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.PriorityQueue;

public  class topnHotItems extends KeyedProcessFunction<Tuple1<String>, resultEntity, String> {
    private final int topSize;
    //  Set<ObjectEntity> allSet = new TreeSet<ObjectEntity>() ;
    private PriorityQueue<sessionResultEntity> sessionOrderEntity ;
    private PriorityQueue<packetResultEntity> packetOrderEntity ;
    private PriorityQueue<byteResultEntity> byteOrderEntity ;


    public topnHotItems(int i) {
        this.topSize = i;



    }
    @Override
    public void open(Configuration parameters) {

        this.sessionOrderEntity=new PriorityQueue<>();
        this.packetOrderEntity=new PriorityQueue<>();
        this.byteOrderEntity=new PriorityQueue<>();


    }


    @Override
    public void processElement(resultEntity objectEntity, Context context, Collector<String> collector)  {




        switch(objectEntity.getOrder_by()) {
            case "sessions":


                if (sessionOrderEntity.size() < topSize) {
                    sessionOrderEntity.add(objectEntity.getSessionResultEntity());
                } else {
                    if (sessionOrderEntity.peek() != null) {
                        sessionResultEntity res=sessionOrderEntity.peek();
                        if (res.getSessions() <= objectEntity.getSessionResultEntity().getSessions()) {
                            sessionOrderEntity.poll();
                            sessionOrderEntity.add(objectEntity.getSessionResultEntity());
                        }
                    }
                }

                break;
            case "packets":
                    if (packetOrderEntity.size() < topSize) {
                        packetOrderEntity.add(objectEntity.getPacketResultEntity());
                    } else {
                    if (packetOrderEntity.peek() != null) {
                        packetResultEntity res=packetOrderEntity.peek();
                        if ((res.getIn_pkts()+res.getOut_pkts()) <= (objectEntity.getPacketResultEntity().getIn_pkts()+objectEntity.getPacketResultEntity().getOut_pkts())) {
                            packetOrderEntity.poll();
                            packetOrderEntity.add(objectEntity.getPacketResultEntity());
                        }
                    }
                }
                break;
            case "bytes":
                if (byteOrderEntity.size() < topSize) {
                    byteOrderEntity.add(objectEntity.getByteResultEntity());
                } else {
                    if (byteOrderEntity.peek() != null) {
                        byteResultEntity res=byteOrderEntity.peek();
                        if ((res.getIn_bytes()+res.getOut_bytes()) <= (objectEntity.getByteResultEntity().getIn_bytes()+objectEntity.getByteResultEntity().getOut_bytes())) {
                            byteOrderEntity.poll();
                            byteOrderEntity.add(objectEntity.getByteResultEntity());
                        }
                    }
                }
                break;
            default:

        }

        //注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收集好了所有 windowEnd的商品数据
        context.timerService().registerEventTimeTimer(objectEntity.getStat_time() + 1);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)  {

            for(sessionResultEntity en : sessionOrderEntity) {

                metricsEntity metricsEntity = new metricsEntity();
                metricsEntity.setName("sessions_top_" + en.getKey_by());
                Map tags = new LinkedMap();
                tags.put("vsys_id", en.getVsys_id());
                tags.put("device_id", en.getDevice_id());
                tags.put("device_group", en.getDevice_group());
                tags.put("data_center", en.getData_center());
                switch (en.getKey_by()) {
                    case "client_ip":
                        tags.put("client_ip", en.getClient_ip());
                        break;
                    case "server_ip":
                        tags.put("server_ip", en.getServer_ip());
                        break;
                    case "internal_ip":
                        tags.put("internal_ip", en.getInternal_ip());
                        break;
                    case "external_ip":
                        tags.put("external_ip", en.getExternal_ip());
                        break;
                    case "server_domain":
                        tags.put("domain", en.getDomain());
                        break;
                    case "subscriber_id":
                        tags.put("subscriber_id", en.getSubscriber_id());
                        break;
                    case "server_fqdn":
                        tags.put("fqdn", en.getFqdn());
                        break;
                    default:
                        break;
                }
                    Map fields = new LinkedMap();
                    fields.put("sessions", en.getSessions());
                    fields.put("out_bytes", en.getOut_bytes());
                    fields.put("in_bytes", en.getIn_bytes());
                    fields.put("out_pkts", en.getOut_pkts());
                    fields.put("in_pkts", en.getIn_pkts());
                    metricsEntity.setFields(fields);
                    metricsEntity.setTags(tags);
                    metricsEntity.setTimestamp_ms(en.getTimestamp_ms());
                    String jsonStr = JSONObject.toJSONString(metricsEntity);
                    out.collect(jsonStr);


            }

        for(packetResultEntity en : packetOrderEntity){


            metricsEntity metricsEntity = new metricsEntity();
            metricsEntity.setName("packets_top_" + en.getKey_by());
            Map tags = new LinkedMap();
            tags.put("vsys_id", en.getVsys_id());
            tags.put("device_id", en.getDevice_id());

            tags.put("device_group", en.getDevice_group());
            tags.put("data_center", en.getData_center());
            switch (en.getKey_by()) {
                case "client_ip":
                    tags.put("client_ip", en.getClient_ip());
                    break;
                case "server_ip":
                    tags.put("server_ip", en.getServer_ip());
                    break;
                case "internal_ip":
                    tags.put("internal_ip", en.getInternal_ip());
                    break;
                case "external_ip":
                    tags.put("external_ip", en.getExternal_ip());
                    break;
                case "server_domain":
                    tags.put("domain", en.getDomain());
                    break;
                case "subscriber_id":
                    tags.put("subscriber_id", en.getSubscriber_id());
                    break;
                case "server_fqdn":
                    tags.put("fqdn", en.getFqdn());
                    break;
                default:
                    break;
            }
            Map fields = new LinkedMap();
            fields.put("sessions", en.getSessions());
            fields.put("out_bytes", en.getOut_bytes());
            fields.put("in_bytes", en.getIn_bytes());
            fields.put("out_pkts", en.getOut_pkts());
            fields.put("in_pkts", en.getIn_pkts());
            metricsEntity.setFields(fields);
            metricsEntity.setTags(tags);
            metricsEntity.setTimestamp_ms(en.getTimestamp_ms());
            String jsonStr = JSONObject.toJSONString(metricsEntity);
            out.collect(jsonStr);


        }
        for(byteResultEntity en : byteOrderEntity){







            metricsEntity metricsEntity = new metricsEntity();
            metricsEntity.setName("bytes_top_" + en.getKey_by());
            Map tags = new LinkedMap();
            tags.put("vsys_id", en.getVsys_id());
            tags.put("device_id", en.getDevice_id());
            tags.put("device_group", en.getDevice_group());
            tags.put("data_center", en.getData_center());
            switch (en.getKey_by()) {
                case "client_ip":
                    tags.put("client_ip", en.getClient_ip());
                    break;
                case "server_ip":
                    tags.put("server_ip", en.getServer_ip());
                    break;
                case "internal_ip":
                    tags.put("internal_ip", en.getInternal_ip());
                    break;
                case "external_ip":
                    tags.put("external_ip", en.getExternal_ip());
                    break;
                case "server_domain":
                    tags.put("domain", en.getDomain());
                    break;
                case "subscriber_id":
                    tags.put("subscriber_id", en.getSubscriber_id());
                    break;
                case "server_fqdn":
                    tags.put("fqdn", en.getFqdn());
                    break;
                default:
                    break;
            }
            Map fields = new LinkedMap();
            fields.put("sessions", en.getSessions());
            fields.put("out_bytes", en.getOut_bytes());
            fields.put("in_bytes", en.getIn_bytes());
            fields.put("out_pkts", en.getOut_pkts());
            fields.put("in_pkts", en.getIn_pkts());
            metricsEntity.setFields(fields);
            metricsEntity.setTags(tags);
            metricsEntity.setTimestamp_ms(en.getTimestamp_ms());
            String jsonStr = JSONObject.toJSONString(metricsEntity);
            out.collect(jsonStr);

        }

        sessionOrderEntity.clear();
        packetOrderEntity.clear();
        byteOrderEntity.clear();



    }
}