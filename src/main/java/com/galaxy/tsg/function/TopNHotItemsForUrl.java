package com.galaxy.tsg.function;

import com.alibaba.fastjson.JSONObject;
import com.galaxy.tsg.pojo.ResultEntity;
import com.galaxy.tsg.pojo.TopUrlEntity;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.PriorityQueue;

public  class TopNHotItemsForUrl extends KeyedProcessFunction<Tuple1<String>, ResultEntity, String> {
    private final int topSize;
    //  Set<ObjectEntity> allSet = new TreeSet<ObjectEntity>() ;
    private PriorityQueue<TopUrlEntity> sessionOrderEntity ;


    public TopNHotItemsForUrl(int i) {
        this.topSize = i;



    }
    @Override
    public void open(Configuration parameters) {

        this.sessionOrderEntity=new PriorityQueue<>();

    }


    @Override
    public void processElement(ResultEntity objectEntity, Context context, Collector<String> collector)  {

        if (sessionOrderEntity.size() < topSize) {
            sessionOrderEntity.add(objectEntity.getTopUrlEntity());
        } else {
            if (sessionOrderEntity.peek() != null) {
                TopUrlEntity res=sessionOrderEntity.peek();
                if (res.getSession_num() <= objectEntity.getTopUrlEntity().getSession_num()) {
                    sessionOrderEntity.poll();
                    sessionOrderEntity.add(objectEntity.getTopUrlEntity());
                }
            }
        }


        //注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收集好了所有 windowEnd的商品数据
        context.timerService().registerEventTimeTimer(objectEntity.getStat_time() + 1);
    }


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)  {

            for(TopUrlEntity en : sessionOrderEntity){

                String jsonStr = JSONObject.toJSONString(en);
                out.collect(jsonStr);

            }


        sessionOrderEntity.clear();

    }
}