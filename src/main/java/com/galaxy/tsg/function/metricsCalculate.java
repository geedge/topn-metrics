package com.galaxy.tsg.function;

import com.galaxy.tsg.pojo.*;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.PriorityQueue;

public  class metricsCalculate extends ProcessWindowFunction<
        Entity,                  // 输入类型
        ResultEntity,  // 输出类型
        Tuple4<String,Long,String,String>,                         // 键类型
        TimeWindow> {                   // 窗口类型
    private final int topSize;
    private final String key;
    //  Set<ObjectEntity> allSet = new TreeSet<ObjectEntity>() ;
    private PriorityQueue<SessionResultEntity> sessionOrderEntity ;
    private PriorityQueue<PacketResultEntity> packetOrderEntity ;
    private PriorityQueue<ByteResultEntity> byteOrderEntity ;

    public metricsCalculate(int i,String key) {
        this.key = key;
        this.topSize = i;
        this.sessionOrderEntity=new PriorityQueue<>();
        this.packetOrderEntity=new PriorityQueue<>();
        this.byteOrderEntity=new PriorityQueue<>();
    }

    @Override
    public void process(Tuple4<String,Long,String,String> s,
                        Context context,
                        Iterable<Entity> elements, Collector<ResultEntity> out) throws Exception {




        for(Entity objectEntity : elements) {

            if (sessionOrderEntity.size() < topSize) {
                sessionOrderEntity.add(enrichessionResult(context.window().getEnd() / 1000, objectEntity));
            } else {
                if (sessionOrderEntity.peek() != null) {
                    SessionResultEntity res = sessionOrderEntity.peek();
                    if (res.getSession_num() <= objectEntity.getCommon_sessions()) {
                        sessionOrderEntity.poll();
                        sessionOrderEntity.add(enrichessionResult(context.window().getEnd() / 1000, objectEntity));
                    }
                }
            }


            if (packetOrderEntity.size() < topSize) {
                packetOrderEntity.add(enrichPacketResult(context.window().getEnd() / 1000, objectEntity));
            } else {
                if (packetOrderEntity.peek() != null) {
                    PacketResultEntity res = packetOrderEntity.peek();
                    if ((res.getS2c_pkt_num() + res.getC2s_pkt_num()) <= (objectEntity.getCommon_s2c_pkt_num() + objectEntity.getCommon_c2s_pkt_num())) {
                        packetOrderEntity.poll();
                        packetOrderEntity.add(enrichPacketResult(context.window().getEnd() / 1000, objectEntity));
                    }
                }
            }

            if (byteOrderEntity.size() < topSize) {
                byteOrderEntity.add(enrichByteResult(context.window().getEnd() / 1000, objectEntity));
            } else {
                if (byteOrderEntity.peek() != null) {
                    ByteResultEntity res = byteOrderEntity.peek();
                    if ((res.getS2c_byte_num() + res.getC2s_byte_num()) <= (objectEntity.getCommon_s2c_byte_num() + objectEntity.getCommon_c2s_byte_num())) {
                        byteOrderEntity.poll();
                        byteOrderEntity.add(enrichByteResult(context.window().getEnd() / 1000, objectEntity));
                    }
                }
            }

        }

            while (sessionOrderEntity.size() > 0) {
                SessionResultEntity obj = sessionOrderEntity.peek();
                //String jsonStr = JSONObject.toJSONString(en);
                ResultEntity en = new ResultEntity();
                en.setOrder_by("sessions");
                en.setStat_time(context.window().getEnd() / 1000);
                en.setSessionResultEntity(obj);
                out.collect(en);
                sessionOrderEntity.remove();
            }


            while (packetOrderEntity.size() > 0) {
                PacketResultEntity obj = packetOrderEntity.peek();
                ResultEntity en = new ResultEntity();
                en.setOrder_by("packets");
                en.setStat_time(context.window().getEnd() / 1000);
                en.setPacketResultEntity(obj);
                out.collect(en);
                packetOrderEntity.remove();
            }

            while (byteOrderEntity.size() > 0) {
                ByteResultEntity obj = byteOrderEntity.peek();
                ResultEntity en = new ResultEntity();
                en.setOrder_by("bytes");
                en.setStat_time(context.window().getEnd() / 1000);
                en.setByteResultEntity(obj);
                out.collect(en);
                byteOrderEntity.remove();
            }

    }

    public ByteResultEntity enrichByteResult(Long time,Entity objectEntity) {
        ByteResultEntity en = new ByteResultEntity();
        en.setVsys_id(objectEntity.getCommon_vsys_id());
        en.setStat_time(time);
        en.setSource(objectEntity.getCommon_client_ip());
        en.setSession_num(objectEntity.getCommon_sessions());
        en.setOrder_by("bytes");
        en.setDevice_group(objectEntity.getCommon_device_group());
        en.setData_center(objectEntity.getCommon_data_center());
        en.setS2c_pkt_num(objectEntity.getCommon_s2c_pkt_num());
        en.setC2s_pkt_num(objectEntity.getCommon_c2s_pkt_num());
        en.setS2c_byte_num(objectEntity.getCommon_s2c_byte_num());
        en.setC2s_byte_num(objectEntity.getCommon_c2s_byte_num());
        switch (key) {
            case "common_client_ip":
                en.setSource(objectEntity.getCommon_client_ip());
                break;
            case "common_server_ip":
                en.setDestination(objectEntity.getCommon_server_ip());
                break;
            case "common_internal_ip":
                en.setSource(objectEntity.getCommon_internal_ip());
                break;
            case "common_external_ip":
                en.setDestination(objectEntity.getCommon_external_ip());
                break;
            case "http_domain":
                en.setDomain(objectEntity.getHttp_domain());
                break;

            case "common_subscriber_id":
                en.setSubscriber_id(objectEntity.getCommon_subscriber_id());
                break;

            case "common_app_label":
                en.setApp_name(objectEntity.getCommon_app_label());
                break;

            default:

        }
        return en;

    }
    public SessionResultEntity enrichessionResult(Long time,Entity objectEntity){

        SessionResultEntity en =new SessionResultEntity();
        en.setVsys_id(objectEntity.getCommon_vsys_id());
        en.setStat_time(time);
        en.setSession_num(objectEntity.getCommon_sessions());
        en.setOrder_by("sessions");
        en.setDevice_group(objectEntity.getCommon_device_group());
        en.setData_center(objectEntity.getCommon_data_center());
        en.setS2c_pkt_num(objectEntity.getCommon_s2c_pkt_num());
        en.setC2s_pkt_num(objectEntity.getCommon_c2s_pkt_num());
        en.setS2c_byte_num(objectEntity.getCommon_s2c_byte_num());
        en.setC2s_byte_num(objectEntity.getCommon_c2s_byte_num());
        switch(key) {
            case "common_client_ip":
                en.setSource(objectEntity.getCommon_client_ip());
                break;
            case "common_server_ip":
                en.setDestination(objectEntity.getCommon_server_ip());
                break;
            case "common_internal_ip":
                en.setSource(objectEntity.getCommon_internal_ip());
                break;
            case "common_external_ip":
                en.setDestination(objectEntity.getCommon_external_ip());
                break;
            case "http_domain":
                en.setDomain(objectEntity.getHttp_domain());
                break;

            case "common_subscriber_id":
                en.setSubscriber_id(objectEntity.getCommon_subscriber_id());
                break;

            case "common_app_label":
                en.setApp_name(objectEntity.getCommon_app_label());
                break;

            default:



        }
         return en;
    }
    public PacketResultEntity enrichPacketResult(Long time,Entity objectEntity){
        PacketResultEntity en =new PacketResultEntity();
        en.setVsys_id(objectEntity.getCommon_vsys_id());
        en.setStat_time(time);
        en.setSource(objectEntity.getCommon_client_ip());
        en.setSession_num(objectEntity.getCommon_sessions());
        en.setOrder_by("packets");
        en.setDevice_group(objectEntity.getCommon_device_group());
        en.setData_center(objectEntity.getCommon_data_center());
        en.setS2c_pkt_num(objectEntity.getCommon_s2c_pkt_num());
        en.setC2s_pkt_num(objectEntity.getCommon_c2s_pkt_num());
        en.setS2c_byte_num(objectEntity.getCommon_s2c_byte_num());
        en.setC2s_byte_num(objectEntity.getCommon_c2s_byte_num());
        switch(key) {
            case "common_client_ip":
                en.setSource(objectEntity.getCommon_client_ip());
                break;
            case "common_server_ip":
                en.setDestination(objectEntity.getCommon_server_ip());
                break;
            case "common_internal_ip":
                en.setSource(objectEntity.getCommon_internal_ip());
                break;
            case "common_external_ip":
                en.setDestination(objectEntity.getCommon_external_ip());
                break;
            case "common_subscriber_id":
                en.setSubscriber_id(objectEntity.getCommon_subscriber_id());
                break;

            case "common_app_label":
                en.setApp_name(objectEntity.getCommon_app_label());
                break;

            default:


        }
        return en;
    }
}