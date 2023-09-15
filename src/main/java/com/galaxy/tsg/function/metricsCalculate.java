package com.galaxy.tsg.function;

import com.galaxy.tsg.pojo.*;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.PriorityQueue;

public class metricsCalculate extends ProcessWindowFunction<
        transformEntity,                  // 输入类型
        resultEntity,  // 输出类型
        Tuple5<String, Long, String, String, String>,                         // 键类型
        TimeWindow> {                   // 窗口类型
    private final int topSize;
    private final String key;
    //  Set<ObjectEntity> allSet = new TreeSet<ObjectEntity>() ;
    private PriorityQueue<sessionResultEntity> sessionOrderEntity;
    private PriorityQueue<packetResultEntity> packetOrderEntity;
    private PriorityQueue<byteResultEntity> byteOrderEntity;

    private long windowStartTimestamp;

    public metricsCalculate(int i, String key) {
        this.key = key;
        this.topSize = i;
        this.sessionOrderEntity = new PriorityQueue<>();
        this.packetOrderEntity = new PriorityQueue<>();
        this.byteOrderEntity = new PriorityQueue<>();
    }

    @Override
    public void process(Tuple5<String, Long, String, String, String> s,
                        Context context,
                        Iterable<transformEntity> elements, Collector<resultEntity> out) throws Exception {

        if (context.window().getStart() == windowStartTimestamp) {



            if (elements.iterator().hasNext()) {


                transformEntity objectTransformEntity = elements.iterator().next();

                if (sessionOrderEntity.size() < topSize) {
                    sessionOrderEntity.add(enrichessionResult(context.window().getStart(), objectTransformEntity));
                } else {
                    if (sessionOrderEntity.peek() != null) {
                        sessionResultEntity res = sessionOrderEntity.peek();
                        if (res.getSessions() <= objectTransformEntity.getSessions()) {
                            sessionOrderEntity.poll();
                            sessionOrderEntity.add(enrichessionResult(context.window().getStart(), objectTransformEntity));
                        }
                    }
                }

                if (packetOrderEntity.size() < topSize) {
                    packetOrderEntity.add(enrichPacketResult(context.window().getStart(), objectTransformEntity));
                } else {
                    if (packetOrderEntity.peek() != null) {
                        packetResultEntity res = packetOrderEntity.peek();
                        if ((res.getIn_pkts() + res.getOut_pkts()) <= (objectTransformEntity.getIn_pkts() + objectTransformEntity.getOut_pkts())) {
                            packetOrderEntity.poll();
                            packetOrderEntity.add(enrichPacketResult(context.window().getStart() , objectTransformEntity));
                        }
                    }
                }

                if (byteOrderEntity.size() < topSize) {
                    byteOrderEntity.add(enrichByteResult(context.window().getStart(), objectTransformEntity));
                } else {
                    if (byteOrderEntity.peek() != null) {
                        byteResultEntity res = byteOrderEntity.peek();
                        if ((res.getIn_bytes() + res.getOut_bytes()) <= (objectTransformEntity.getIn_bytes() + objectTransformEntity.getOut_bytes())) {
                            byteOrderEntity.poll();
                            byteOrderEntity.add(enrichByteResult(context.window().getStart(), objectTransformEntity));
                        }
                    }
                }


            }
        } else {

            for (com.galaxy.tsg.pojo.sessionResultEntity sessionResultEntity : sessionOrderEntity) {

                resultEntity en = new resultEntity();
                en.setOrder_by("sessions");
                en.setStat_time(windowStartTimestamp);
                en.setSessionResultEntity(sessionResultEntity);
                out.collect(en);
            }
            sessionOrderEntity.clear();


            for (com.galaxy.tsg.pojo.packetResultEntity packetResultEntity : packetOrderEntity) {
                resultEntity en = new resultEntity();
                en.setOrder_by("packets");
                en.setStat_time(windowStartTimestamp);
                en.setPacketResultEntity(packetResultEntity);
                out.collect(en);
            }
            packetOrderEntity.clear();

            for (com.galaxy.tsg.pojo.byteResultEntity byteResultEntity : byteOrderEntity) {
                resultEntity en = new resultEntity();
                en.setOrder_by("bytes");
                en.setStat_time(windowStartTimestamp);
                en.setByteResultEntity(byteResultEntity);
                out.collect(en);
            }
            byteOrderEntity.clear();
            windowStartTimestamp = context.window().getStart();


            if (elements.iterator().hasNext()) {


                transformEntity objectTransformEntity = elements.iterator().next();

                if (sessionOrderEntity.size() < topSize) {
                    sessionOrderEntity.add(enrichessionResult(context.window().getStart(), objectTransformEntity));
                } else {
                    if (sessionOrderEntity.peek() != null) {
                        sessionResultEntity res = sessionOrderEntity.peek();
                        if (res.getSessions() <= objectTransformEntity.getSessions()) {
                            sessionOrderEntity.poll();
                            sessionOrderEntity.add(enrichessionResult(context.window().getStart(), objectTransformEntity));
                        }
                    }
                }

                if (packetOrderEntity.size() < topSize) {
                    packetOrderEntity.add(enrichPacketResult(context.window().getStart(), objectTransformEntity));
                } else {
                    if (packetOrderEntity.peek() != null) {
                        packetResultEntity res = packetOrderEntity.peek();
                        if ((res.getIn_pkts() + res.getOut_pkts()) <= (objectTransformEntity.getIn_pkts() + objectTransformEntity.getOut_pkts())) {
                            packetOrderEntity.poll();
                            packetOrderEntity.add(enrichPacketResult(context.window().getStart() , objectTransformEntity));
                        }
                    }
                }

                if (byteOrderEntity.size() < topSize) {
                    byteOrderEntity.add(enrichByteResult(context.window().getStart(), objectTransformEntity));
                } else {
                    if (byteOrderEntity.peek() != null) {
                        byteResultEntity res = byteOrderEntity.peek();
                        if ((res.getIn_bytes() + res.getOut_bytes()) <= (objectTransformEntity.getIn_bytes() + objectTransformEntity.getOut_bytes())) {
                            byteOrderEntity.poll();
                            byteOrderEntity.add(enrichByteResult(context.window().getStart(), objectTransformEntity));
                        }
                    }
                }


            }
        }
    }

    public byteResultEntity enrichByteResult(Long time, transformEntity objectTransformEntity) {
        byteResultEntity en = new byteResultEntity();
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

    public sessionResultEntity enrichessionResult(Long time, transformEntity objectTransformEntity) {

        sessionResultEntity en = new sessionResultEntity();
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

    public packetResultEntity enrichPacketResult(Long time, transformEntity objectTransformEntity) {
        packetResultEntity en = new packetResultEntity();
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