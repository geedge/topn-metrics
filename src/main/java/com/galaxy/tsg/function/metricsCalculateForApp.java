package com.galaxy.tsg.function;

import com.alibaba.fastjson.JSONObject;
import com.galaxy.tsg.pojo.AppEntity;
import com.galaxy.tsg.pojo.Entity;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public  class metricsCalculateForApp extends ProcessWindowFunction<
        Entity,                  // 输入类型
        String,  // 输出类型
        Tuple4<String,Long,String,String>,                         // 键类型
        TimeWindow> {                   // 窗口类型



    @Override
    public void process(Tuple4<String,Long,String,String> s,
                        Context context,
                        Iterable<Entity> elements, Collector<String> out) throws Exception {


        for (Entity objectEntity: elements) {
            AppEntity en =new AppEntity();
            en.setVsys_id(objectEntity.getCommon_vsys_id());
            en.setStat_time(context.window().getEnd() / 1000);
            en.setSession_num(objectEntity.getCommon_sessions());
            en.setApp_name(objectEntity.getCommon_app_label());
            en.setDevice_group(objectEntity.getCommon_device_group());
            en.setData_center(objectEntity.getCommon_data_center());
            en.setS2c_pkt_num(objectEntity.getCommon_s2c_pkt_num());
            en.setC2s_pkt_num(objectEntity.getCommon_c2s_pkt_num());
            en.setS2c_byte_num(objectEntity.getCommon_s2c_byte_num());
            en.setC2s_byte_num(objectEntity.getCommon_c2s_byte_num());
            String jsonStr = JSONObject.toJSONString(en);
            out.collect(jsonStr);
        }


    }
}