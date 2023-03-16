package com.galaxy.tsg.function;

import com.galaxy.tsg.pojo.*;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public  class metricsCalculateTest extends ProcessWindowFunction<
        Tuple1<HashMap<String, Entity>>,                  // 输入类型
        ResultEntity,  // 输出类型
        Tuple4<String,Long,String,String>,                         // 键类型
        TimeWindow> {                   // 窗口类型
    private final int topSize;
    private final String key;
    //  Set<ObjectEntity> allSet = new TreeSet<ObjectEntity>() ;
    private PriorityQueue<SessionResultEntity> sessionOrderEntity ;
    private PriorityQueue<PacketResultEntity> packetOrderEntity ;
    private PriorityQueue<ByteResultEntity> byteOrderEntity ;

    public metricsCalculateTest(int i, String key) {
        this.key = key;
        this.topSize = i;
        this.sessionOrderEntity=new PriorityQueue<>();
        this.packetOrderEntity=new PriorityQueue<>();
        this.byteOrderEntity=new PriorityQueue<>();
    }

    @Override
    public void process(Tuple4<String,Long,String,String> s,
                        Context context,
                        Iterable<Tuple1<HashMap<String, Entity>>> elements, Collector<ResultEntity> out) throws Exception {





        for(Tuple1<HashMap<String, Entity>> objectEntity : elements) {

            for (Map.Entry<String, Entity>  entry: objectEntity.f0.entrySet()){



                ByteResultEntity testEntity = enrichByteResult(context.window().getEnd() / 1000,entry.getValue());

                ResultEntity resultEntity = new ResultEntity();
                resultEntity.setOrder_by("");
                resultEntity.setByteResultEntity(testEntity);
                resultEntity.setStat_time(context.window().getEnd() / 1000);
                out.collect(resultEntity);


            }

        }


    }

    public ByteResultEntity enrichByteResult(Long time,Entity objectEntity) {
        ByteResultEntity en = new ByteResultEntity();
        en.setVsys_id(objectEntity.getCommon_vsys_id());
        en.setStat_time(time);
        en.setSource(objectEntity.getCommon_client_ip());
        en.setSession_num(objectEntity.getCommon_sessions());
        en.setOrder_by("");
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

}