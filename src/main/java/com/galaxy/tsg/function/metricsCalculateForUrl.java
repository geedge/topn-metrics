package com.galaxy.tsg.function;

import com.galaxy.tsg.pojo.ResultEntity;
import com.galaxy.tsg.pojo.TopUrlEntity;
import com.galaxy.tsg.pojo.UrlEntity;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.PriorityQueue;

public  class metricsCalculateForUrl extends ProcessWindowFunction<
        UrlEntity,                  // 输入类型
        ResultEntity,  // 输出类型
        Tuple2<String,Long>,                         // 键类型
        TimeWindow> {                   // 窗口类型
    private final int topSize;
    //  Set<ObjectEntity> allSet = new TreeSet<ObjectEntity>() ;
    private PriorityQueue<TopUrlEntity> sessionOrderEntity;


    public metricsCalculateForUrl(int i) {
        this.topSize = i;
        this.sessionOrderEntity = new PriorityQueue<>();

    }

    @Override
    public void process(Tuple2 < String, Long > s,
                        Context context,
                        Iterable<UrlEntity> elements, Collector<ResultEntity> out) throws Exception {


        for (UrlEntity objectEntity: elements) {
            if (sessionOrderEntity.size() < topSize) {
                TopUrlEntity en = new TopUrlEntity();
                en.setSession_num(objectEntity.getCommon_sessions());
                en.setStat_time(context.window().getEnd() / 1000);
                en.setUrl(objectEntity.getHttp_url());
                en.setVsys_id(objectEntity.getCommon_vsys_id());
                sessionOrderEntity.add(en);
            } else {
                if (sessionOrderEntity.peek() != null) {
                    TopUrlEntity res = sessionOrderEntity.peek();
                    if (res.getSession_num() <= objectEntity.getCommon_sessions()) {
                        sessionOrderEntity.poll();
                        TopUrlEntity en = new TopUrlEntity();
                        en.setSession_num(objectEntity.getCommon_sessions());
                        en.setStat_time(context.window().getEnd() / 1000);
                        en.setUrl(objectEntity.getHttp_url());
                        en.setVsys_id(objectEntity.getCommon_vsys_id());
                        sessionOrderEntity.add(en);
                    }
                }
            }
        }

        while (sessionOrderEntity.size() > 0) {
            TopUrlEntity obj = sessionOrderEntity.peek();
            ResultEntity resultEntity = new ResultEntity();
            resultEntity.setOrder_by("sessions");
            resultEntity.setStat_time(context.window().getEnd() / 1000);
            resultEntity.setTopUrlEntity(obj);
            out.collect(resultEntity);
            sessionOrderEntity.remove();
        }

    }
}