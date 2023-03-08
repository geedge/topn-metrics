package com.galaxy.tsg.function;


import com.galaxy.tsg.pojo.Entity;
import com.galaxy.tsg.pojo.UrlEntity;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.HashMap;

/**
 * @author fy
 * @version 1.0
 * @date 2023/2/14 17:29
 *
 *Session_record top10000  21个窗口计算
 */
public class UserHashMapCountAgg6 implements AggregateFunction<UrlEntity, HashMap<String,DimensionItemsSketch>, HashMap<String,DimensionItemsSketch>> {


    @Override
    public HashMap<String, DimensionItemsSketch> createAccumulator() {
        return new HashMap<String, DimensionItemsSketch>(1048576);
    }

    @Override
    public HashMap<String, DimensionItemsSketch> add(UrlEntity cnRecordLog, HashMap<String, DimensionItemsSketch> stringItemsSketchHashMap) {

//        String dimension = cnRecordLog.getCommon_client_ip();//维度
//        System.out.println(dimension);

        if(stringItemsSketchHashMap.isEmpty()) {

            ItemsSketch<String> eightSessionItemsSketch = new ItemsSketch<>(1048576);//新建

            eightSessionItemsSketch.update(Dimension.setEightDimension(cnRecordLog),cnRecordLog.getCommon_sessions());

            DimensionItemsSketch eightSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.EIGHT,eightSessionItemsSketch);

            stringItemsSketchHashMap.put("eightSession", eightSessionDimensionItemsSketch);


        }else {



            stringItemsSketchHashMap.get("eightSession").getItemsSketch().update(Dimension.setEightDimension(cnRecordLog),cnRecordLog.getCommon_sessions());




        }



        return stringItemsSketchHashMap;
    }

    @Override
    public HashMap<String, DimensionItemsSketch> getResult(HashMap<String, DimensionItemsSketch> stringItemsSketchHashMap) {


        return stringItemsSketchHashMap;
    }

    @Override
    public HashMap<String, DimensionItemsSketch> merge(HashMap<String, DimensionItemsSketch> stringItemsSketchHashMap, HashMap<String, DimensionItemsSketch> acc1) {

//        System.out.println("合并");
//        HashMap<String, ItemsSketch<String>> unionSketchHashMap = new HashMap<>();
//        ItemsSketch<String> sessionItemsSketch = new ItemsSketch<>(1048576);
//        ItemsSketch<String> pktItemsSketch = new ItemsSketch<>(1048576);
//        ItemsSketch<String> byteItemsSketch = new ItemsSketch<>(1048576);

//        ItemsSketch<String> session_stringItemsSketch = stringItemsSketchHashMap.get("session");
//        ItemsSketch<String> pkt_stringItemsSketch = stringItemsSketchHashMap.get("pkt");
//        ItemsSketch<String> byte_stringItemsSketch = stringItemsSketchHashMap.get("byte");

//        ItemsSketch<String> session_acc1 = acc1.get("session");
//        ItemsSketch<String> pkt_acc1 = acc1.get("pkt");
//        ItemsSketch<String> byte_acc1 = acc1.get("byte");

//        sessionItemsSketch.merge(session_stringItemsSketch);
//        sessionItemsSketch.merge(session_acc1);

//        pktItemsSketch.merge(pkt_stringItemsSketch);
//        pktItemsSketch.merge(pkt_acc1);
//
//        byteItemsSketch.merge(byte_stringItemsSketch);
//        byteItemsSketch.merge(byte_acc1);

//        unionSketchHashMap.put("session",sessionItemsSketch);
//        unionSketchHashMap.put("pkt",pktItemsSketch);
//        unionSketchHashMap.put("byte",byteItemsSketch);


        return null;
    }
}
