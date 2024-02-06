package com.galaxy.tsg.function;

import com.galaxy.tsg.pojo.*;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MetricsCalculate extends ProcessWindowFunction<
        TransformEntity,                  // 输入类型
        ResultEntity,  // 输出类型
        Tuple5<String, Long, String, String, String>,                         // 键类型
        TimeWindow> {                   // 窗口类型


    @Override
    public void process(Tuple5<String, Long, String, String, String> s,
                        Context context,
                        Iterable<TransformEntity> elements, Collector<ResultEntity> out) throws Exception {


        if (elements.iterator().hasNext()) {

            TransformEntity objectTransformEntity = elements.iterator().next();
            ResultEntity enSession = new ResultEntity();
            enSession.setOrder_by("sessions");
            enSession.setStat_time(context.window().getStart());
            enSession.setSessionResultEntity(enrichessionResult(context.window().getStart(), objectTransformEntity));
            out.collect(enSession);

            ResultEntity enPacket = new ResultEntity();
            enPacket.setOrder_by("packets");
            enPacket.setStat_time(context.window().getStart());
            enPacket.setPacketResultEntity(enrichPacketResult(context.window().getStart() , objectTransformEntity));
            out.collect(enPacket);

            ResultEntity enbyte = new ResultEntity();
            enbyte.setOrder_by("bytes");
            enbyte.setStat_time(context.window().getStart());
            enbyte.setByteResultEntity(enrichByteResult(context.window().getStart(), objectTransformEntity));
            out.collect(enbyte);


        }


    }

    public ByteResultEntity enrichByteResult(Long time, TransformEntity objectTransformEntity) {
        ByteResultEntity en = new ByteResultEntity();
        en.setVsys_id(objectTransformEntity.getVsys_id());
        en.setTimestamp_ms(time);
        en.setSessions(objectTransformEntity.getSessions());
        en.setOrder_by("bytes");

        en.setServer_ip(objectTransformEntity.getServer_ip());
        en.setClient_ip(objectTransformEntity.getClient_ip());
        en.setSubscriber_id(objectTransformEntity.getSubscriber_id());
        en.setFqdn(objectTransformEntity.getFqdn());
        en.setExternal_ip(objectTransformEntity.getExternal_ip());
        en.setInternal_ip(objectTransformEntity.getInternal_ip());
        en.setDomain(objectTransformEntity.getDomain());
        en.setDevice_group(objectTransformEntity.getDevice_group());
        en.setData_center(objectTransformEntity.getData_center());
        en.setIn_bytes(objectTransformEntity.getIn_bytes());
        en.setIn_pkts(objectTransformEntity.getIn_pkts());
        en.setOut_bytes(objectTransformEntity.getOut_bytes());
        en.setOut_pkts(objectTransformEntity.getOut_pkts());
        en.setKey_by(objectTransformEntity.getKey_by());

        return en;

    }

    public SessionResultEntity enrichessionResult(Long time, TransformEntity objectTransformEntity) {

        SessionResultEntity en = new SessionResultEntity();
        en.setVsys_id(objectTransformEntity.getVsys_id());
        en.setTimestamp_ms(time);
        en.setSessions(objectTransformEntity.getSessions());
        en.setOrder_by("sessions");
        en.setServer_ip(objectTransformEntity.getServer_ip());
        en.setClient_ip(objectTransformEntity.getClient_ip());
        en.setSubscriber_id(objectTransformEntity.getSubscriber_id());
        en.setFqdn(objectTransformEntity.getFqdn());
        en.setExternal_ip(objectTransformEntity.getExternal_ip());
        en.setInternal_ip(objectTransformEntity.getInternal_ip());
        en.setDomain(objectTransformEntity.getDomain());
        en.setDevice_group(objectTransformEntity.getDevice_group());
        en.setData_center(objectTransformEntity.getData_center());
        en.setIn_bytes(objectTransformEntity.getIn_bytes());
        en.setIn_pkts(objectTransformEntity.getIn_pkts());
        en.setOut_bytes(objectTransformEntity.getOut_bytes());
        en.setOut_pkts(objectTransformEntity.getOut_pkts());
        en.setKey_by(objectTransformEntity.getKey_by());
        return en;
    }

    public PacketResultEntity enrichPacketResult(Long time, TransformEntity objectTransformEntity) {
        PacketResultEntity en = new PacketResultEntity();
        en.setVsys_id(objectTransformEntity.getVsys_id());
        en.setTimestamp_ms(time);
        en.setSessions(objectTransformEntity.getSessions());
        en.setOrder_by("packets");
        en.setServer_ip(objectTransformEntity.getServer_ip());
        en.setClient_ip(objectTransformEntity.getClient_ip());
        en.setSubscriber_id(objectTransformEntity.getSubscriber_id());
        en.setFqdn(objectTransformEntity.getFqdn());
        en.setExternal_ip(objectTransformEntity.getExternal_ip());
        en.setInternal_ip(objectTransformEntity.getInternal_ip());
        en.setDomain(objectTransformEntity.getDomain());
        en.setDevice_group(objectTransformEntity.getDevice_group());
        en.setData_center(objectTransformEntity.getData_center());
        en.setIn_bytes(objectTransformEntity.getIn_bytes());
        en.setIn_pkts(objectTransformEntity.getIn_pkts());
        en.setOut_bytes(objectTransformEntity.getOut_bytes());
        en.setOut_pkts(objectTransformEntity.getOut_pkts());
        en.setKey_by(objectTransformEntity.getKey_by());

        return en;
    }
}