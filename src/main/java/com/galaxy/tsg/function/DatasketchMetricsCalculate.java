package com.galaxy.tsg.function;

import com.alibaba.fastjson.JSONObject;
import com.galaxy.tsg.pojo.*;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;

import static com.galaxy.tsg.config.commonConfig.TOP_LIMIT;

/**
 * @author fy
 * @version 1.0
 * @date 2023/3/10 16:09
 */
public class DatasketchMetricsCalculate extends ProcessWindowFunction<
        Entity,                  // 输入类型
        ResultEntity,  // 输出类型
        Tuple4<String,Long,String,String>,                         // 键类型
        TimeWindow> {

    private final int topSize;
    private final String key;




    public DatasketchMetricsCalculate(int topSize, String key) {
        this.topSize = topSize;
        this.key = key;
    }


    @Override
    public void process(Tuple4<String, Long, String, String> S, Context context, Iterable<Entity> iterable, Collector<ResultEntity> collector) throws Exception {


        ItemsSketch<String> sessionItemsSketch = new ItemsSketch<>(1048576);//新建
        ItemsSketch<String> pktItemsSketch = new ItemsSketch<>(1048576);//新建
        ItemsSketch<String> byteItemsSketch = new ItemsSketch<>(1048576);//新建

        HashMap<String, SessionResultEntity> sessionResultEntityHashMap = new HashMap<>();
        HashMap<String, PacketResultEntity> packetResultEntityHashMap = new HashMap<>();
        HashMap<String, ByteResultEntity> byteResultEntityHashMap = new HashMap<>();



        for (Entity entity:iterable){

            //处理session
            sessionItemsSketch.update(Dimension.setDimension(entity,key),entity.getCommon_sessions());
            sessionResultEntityHashMap.put(Dimension.setDimension(entity,key),enrichessionResult(context.window().getEnd() / 1000, entity));

            //处理pkt
            pktItemsSketch.update(Dimension.setDimension(entity,key),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());
            packetResultEntityHashMap.put(Dimension.setDimension(entity,key),enrichPacketResult(context.window().getEnd() / 1000, entity));

            //处理byte
            byteItemsSketch.update(Dimension.setDimension(entity,key),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());
            byteResultEntityHashMap.put(Dimension.setDimension(entity,key),enrichByteResult(context.window().getEnd() / 1000, entity));


        }


        ItemsSketch.Row<String>[] sessionFrequentItems = sessionItemsSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);

        for (int i=0;i<sessionFrequentItems.length;i++){

            SessionResultEntity sessionResultEntity = sessionResultEntityHashMap.get(sessionFrequentItems[i].getItem());

            ResultEntity en = new ResultEntity();
            en.setOrder_by("sessions");
            en.setStat_time(context.window().getEnd() / 1000);
            en.setSessionResultEntity(sessionResultEntity);
            collector.collect(en);


            if (i==topSize)//够条数就结束
                break;

        }


        ItemsSketch.Row<String>[] pktFrequentItems = pktItemsSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);

        for (int i=0;i<pktFrequentItems.length;i++){

            PacketResultEntity packetResultEntity = packetResultEntityHashMap.get(pktFrequentItems[i].getItem());

            ResultEntity en = new ResultEntity();
            en.setOrder_by("packets");
            en.setStat_time(context.window().getEnd() / 1000);
            en.setPacketResultEntity(packetResultEntity);
            collector.collect(en);


            if (i==topSize)//够条数就结束
                break;

        }


        ItemsSketch.Row<String>[] byteFrequentItems = byteItemsSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);

        for (int i=0;i<byteFrequentItems.length;i++){

            ByteResultEntity byteResultEntity = byteResultEntityHashMap.get(byteFrequentItems[i].getItem());

            ResultEntity en = new ResultEntity();
            en.setOrder_by("bytes");
            en.setStat_time(context.window().getEnd() / 1000);
            en.setByteResultEntity(byteResultEntity);
            collector.collect(en);


            if (i==TOP_LIMIT)//够条数就结束
                break;

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
