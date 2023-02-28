package com.galaxy.tsg.function;

import com.alibaba.fastjson.JSONObject;
import com.galaxy.tsg.pojo.ByteResultEntity;
import com.galaxy.tsg.pojo.PacketResultEntity;
import com.galaxy.tsg.pojo.ResultEntity;
import com.galaxy.tsg.pojo.SessionResultEntity;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.PriorityQueue;

public  class TopNHotItems extends KeyedProcessFunction<Tuple1<String>, ResultEntity, String> {
    private final int topSize;
    //  Set<ObjectEntity> allSet = new TreeSet<ObjectEntity>() ;
    private PriorityQueue<SessionResultEntity> sessionOrderEntity ;
    private PriorityQueue<PacketResultEntity> packetOrderEntity ;
    private PriorityQueue<ByteResultEntity> byteOrderEntity ;


    public TopNHotItems(int i) {
        this.topSize = i;



    }
    @Override
    public void open(Configuration parameters) {

        this.sessionOrderEntity=new PriorityQueue<>();
        this.packetOrderEntity=new PriorityQueue<>();
        this.byteOrderEntity=new PriorityQueue<>();


    }


    @Override
    public void processElement(ResultEntity objectEntity, Context context, Collector<String> collector)  {




        switch(objectEntity.getOrder_by()) {
            case "sessions":


                if (sessionOrderEntity.size() < topSize) {
                    sessionOrderEntity.add(objectEntity.getSessionResultEntity());
                } else {
                    if (sessionOrderEntity.peek() != null) {
                        SessionResultEntity res=sessionOrderEntity.peek();
                        if (res.getSession_num() <= objectEntity.getSessionResultEntity().getSession_num()) {
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
                        PacketResultEntity res=packetOrderEntity.peek();
                        if ((res.getS2c_pkt_num()+res.getC2s_pkt_num()) <= (objectEntity.getPacketResultEntity().getC2s_pkt_num()+objectEntity.getPacketResultEntity().getS2c_pkt_num())) {
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
                        ByteResultEntity res=byteOrderEntity.peek();
                        if ((res.getC2s_byte_num()+res.getS2c_byte_num()) <= (objectEntity.getByteResultEntity().getS2c_byte_num()+objectEntity.getByteResultEntity().getC2s_byte_num())) {
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

            for(SessionResultEntity en : sessionOrderEntity){

                String jsonStr = JSONObject.toJSONString(en);
                out.collect(jsonStr);

            }


        for(PacketResultEntity en : packetOrderEntity){

            String jsonStr = JSONObject.toJSONString(en);
            out.collect(jsonStr);

        }
        for(ByteResultEntity en : byteOrderEntity){

            String jsonStr = JSONObject.toJSONString(en);
            out.collect(jsonStr);

        }

        sessionOrderEntity.clear();
        packetOrderEntity.clear();
        byteOrderEntity.clear();



    }
}