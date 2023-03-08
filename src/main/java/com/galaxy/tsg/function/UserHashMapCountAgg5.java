package com.galaxy.tsg.function;


import com.galaxy.tsg.pojo.Entity;
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
public class UserHashMapCountAgg5 implements AggregateFunction<Entity, HashMap<String,DimensionItemsSketch>, HashMap<String,DimensionItemsSketch>> {


    @Override
    public HashMap<String, DimensionItemsSketch> createAccumulator() {
        return new HashMap<String, DimensionItemsSketch>(1048576);
    }

    @Override
    public HashMap<String, DimensionItemsSketch> add(Entity cnRecordLog, HashMap<String, DimensionItemsSketch> stringItemsSketchHashMap) {

//        String dimension = cnRecordLog.getCommon_client_ip();//维度
//        System.out.println(dimension);

        if(stringItemsSketchHashMap.isEmpty()) {

            ItemsSketch<String> oneSessionItemsSketch = new ItemsSketch<>(1048576);//新建
            ItemsSketch<String> twoSessionItemsSketch = new ItemsSketch<>(1048576);//新建
            ItemsSketch<String> threeSessionItemsSketch = new ItemsSketch<>(1048576);//新建
            ItemsSketch<String> fourSessionItemsSketch = new ItemsSketch<>(1048576);//新建
            ItemsSketch<String> fiveSessionItemsSketch = new ItemsSketch<>(1048576);//新建
            ItemsSketch<String> sixSessionItemsSketch = new ItemsSketch<>(1048576);//新建
            ItemsSketch<String> sevenSessionItemsSketch = new ItemsSketch<>(1048576);//新建

            ItemsSketch<String> onePktItemsSketch = new ItemsSketch<>(1048576);
            ItemsSketch<String> twoPktItemsSketch = new ItemsSketch<>(1048576);
            ItemsSketch<String> threePktItemsSketch = new ItemsSketch<>(1048576);
            ItemsSketch<String> fourPktItemsSketch = new ItemsSketch<>(1048576);
            ItemsSketch<String> fivePktItemsSketch = new ItemsSketch<>(1048576);
            ItemsSketch<String> sixPktItemsSketch = new ItemsSketch<>(1048576);
            ItemsSketch<String> sevenPktItemsSketch = new ItemsSketch<>(1048576);



            ItemsSketch<String> oneByteItemsSketch = new ItemsSketch<>(1048576);
            ItemsSketch<String> twoByteItemsSketch = new ItemsSketch<>(1048576);
            ItemsSketch<String> threeByteItemsSketch = new ItemsSketch<>(1048576);
            ItemsSketch<String> fourByteItemsSketch = new ItemsSketch<>(1048576);
            ItemsSketch<String> fiveByteItemsSketch = new ItemsSketch<>(1048576);
            ItemsSketch<String> sixByteItemsSketch = new ItemsSketch<>(1048576);
            ItemsSketch<String> sevenByteItemsSketch = new ItemsSketch<>(1048576);


            oneSessionItemsSketch.update(Dimension.setOneDimension(cnRecordLog),cnRecordLog.getCommon_sessions());
            twoSessionItemsSketch.update(Dimension.setTwoDimension(cnRecordLog),cnRecordLog.getCommon_sessions());
            threeSessionItemsSketch.update(Dimension.setThreeDimension(cnRecordLog),cnRecordLog.getCommon_sessions());
            fourSessionItemsSketch.update(Dimension.setFourDimension(cnRecordLog),cnRecordLog.getCommon_sessions());
            fiveSessionItemsSketch.update(Dimension.setFiveDimension(cnRecordLog),cnRecordLog.getCommon_sessions());
            sixSessionItemsSketch.update(Dimension.setSixDimension(cnRecordLog),cnRecordLog.getCommon_sessions());
            sevenSessionItemsSketch.update(Dimension.setSevenDimension(cnRecordLog),cnRecordLog.getCommon_sessions());

            onePktItemsSketch.update(Dimension.setOneDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());
            twoPktItemsSketch.update(Dimension.setTwoDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());
            threePktItemsSketch.update(Dimension.setThreeDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());
            fourPktItemsSketch.update(Dimension.setFourDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());
            fivePktItemsSketch.update(Dimension.setFiveDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());
            sixPktItemsSketch.update(Dimension.setSixDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());
            sevenPktItemsSketch.update(Dimension.setSevenDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());


            oneByteItemsSketch.update(Dimension.setOneDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());
            twoByteItemsSketch.update(Dimension.setTwoDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());
            threeByteItemsSketch.update(Dimension.setThreeDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());
            fourByteItemsSketch.update(Dimension.setFourDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());
            fiveByteItemsSketch.update(Dimension.setFiveDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());
            sixByteItemsSketch.update(Dimension.setSixDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());
            sevenByteItemsSketch.update(Dimension.setSevenDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());



            DimensionItemsSketch oneSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.ONE, oneSessionItemsSketch);
            DimensionItemsSketch twoSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.TWO,twoSessionItemsSketch);
            DimensionItemsSketch threeSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.THREE,threeSessionItemsSketch);
            DimensionItemsSketch fourSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.FOUR,fourSessionItemsSketch);
            DimensionItemsSketch fiveSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.FIVE,fiveSessionItemsSketch);
            DimensionItemsSketch sixSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.SIX,sixSessionItemsSketch);
            DimensionItemsSketch sevenSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.SEVEN,sevenSessionItemsSketch);

            DimensionItemsSketch onePktDimensionItemsSketch = new DimensionItemsSketch(Dimension.ONE,onePktItemsSketch);
            DimensionItemsSketch twoPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.TWO,twoPktItemsSketch);
            DimensionItemsSketch threePktDimensionItemsSketch = new DimensionItemsSketch(Dimension.THREE,threePktItemsSketch);
            DimensionItemsSketch fourPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.FOUR,fourPktItemsSketch);
            DimensionItemsSketch fivePktDimensionItemsSketch = new DimensionItemsSketch(Dimension.FIVE,fivePktItemsSketch);
            DimensionItemsSketch sixPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.SIX,sixPktItemsSketch);
            DimensionItemsSketch sevenPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.SEVEN,sevenPktItemsSketch);

            DimensionItemsSketch oneByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.ONE,oneByteItemsSketch);
            DimensionItemsSketch twoByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.TWO,twoByteItemsSketch);
            DimensionItemsSketch threeByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.THREE,threeByteItemsSketch);
            DimensionItemsSketch fourByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.FOUR,fourByteItemsSketch);
            DimensionItemsSketch fiveByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.FIVE,fiveByteItemsSketch);
            DimensionItemsSketch sixByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.SIX,sixByteItemsSketch);
            DimensionItemsSketch sevenByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.SEVEN,sevenByteItemsSketch);


            stringItemsSketchHashMap.put("oneSession", oneSessionDimensionItemsSketch);
            stringItemsSketchHashMap.put("twoSession", twoSessionDimensionItemsSketch);
            stringItemsSketchHashMap.put("threeSession", threeSessionDimensionItemsSketch);
            stringItemsSketchHashMap.put("fourSession", fourSessionDimensionItemsSketch);
            stringItemsSketchHashMap.put("fiveSession", fiveSessionDimensionItemsSketch);
            stringItemsSketchHashMap.put("sixSession", sixSessionDimensionItemsSketch);
            stringItemsSketchHashMap.put("sevenSession", sevenSessionDimensionItemsSketch);


            stringItemsSketchHashMap.put("onePkt",onePktDimensionItemsSketch);
            stringItemsSketchHashMap.put("twoPkt",twoPktDimensionItemsSketch);
            stringItemsSketchHashMap.put("threePkt",threePktDimensionItemsSketch);
            stringItemsSketchHashMap.put("fourPkt",fourPktDimensionItemsSketch);
            stringItemsSketchHashMap.put("fivePkt",fivePktDimensionItemsSketch);
            stringItemsSketchHashMap.put("sixPkt",sixPktDimensionItemsSketch);
            stringItemsSketchHashMap.put("sevenPkt",sevenPktDimensionItemsSketch);


            stringItemsSketchHashMap.put("oneByte",oneByteDimensionItemsSketch);
            stringItemsSketchHashMap.put("twoByte",twoByteDimensionItemsSketch);
            stringItemsSketchHashMap.put("threeByte",threeByteDimensionItemsSketch);
            stringItemsSketchHashMap.put("fourByte",fourByteDimensionItemsSketch);
            stringItemsSketchHashMap.put("fiveByte",fiveByteDimensionItemsSketch);
            stringItemsSketchHashMap.put("sixByte",sixByteDimensionItemsSketch);
            stringItemsSketchHashMap.put("sevenByte",sevenByteDimensionItemsSketch);


        }else {

            stringItemsSketchHashMap.get("oneSession").getItemsSketch().update(Dimension.setOneDimension(cnRecordLog),cnRecordLog.getCommon_sessions());
            stringItemsSketchHashMap.get("twoSession").getItemsSketch().update(Dimension.setTwoDimension(cnRecordLog),cnRecordLog.getCommon_sessions());
            stringItemsSketchHashMap.get("threeSession").getItemsSketch().update(Dimension.setThreeDimension(cnRecordLog),cnRecordLog.getCommon_sessions());
            stringItemsSketchHashMap.get("fourSession").getItemsSketch().update(Dimension.setFourDimension(cnRecordLog),cnRecordLog.getCommon_sessions());
            stringItemsSketchHashMap.get("fiveSession").getItemsSketch().update(Dimension.setFiveDimension(cnRecordLog),cnRecordLog.getCommon_sessions());
            stringItemsSketchHashMap.get("sixSession").getItemsSketch().update(Dimension.setSixDimension(cnRecordLog),cnRecordLog.getCommon_sessions());
            stringItemsSketchHashMap.get("sevenSession").getItemsSketch().update(Dimension.setSevenDimension(cnRecordLog),cnRecordLog.getCommon_sessions());

            stringItemsSketchHashMap.get("onePkt").getItemsSketch().update(Dimension.setOneDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());
            stringItemsSketchHashMap.get("twoPkt").getItemsSketch().update(Dimension.setTwoDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());
            stringItemsSketchHashMap.get("threePkt").getItemsSketch().update(Dimension.setThreeDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());
            stringItemsSketchHashMap.get("fourPkt").getItemsSketch().update(Dimension.setFourDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());
            stringItemsSketchHashMap.get("fivePkt").getItemsSketch().update(Dimension.setFiveDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());
            stringItemsSketchHashMap.get("sixPkt").getItemsSketch().update(Dimension.setSixDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());
            stringItemsSketchHashMap.get("sevenPkt").getItemsSketch().update(Dimension.setSevenDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());


            stringItemsSketchHashMap.get("oneByte").getItemsSketch().update(Dimension.setOneDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());
            stringItemsSketchHashMap.get("twoByte").getItemsSketch().update(Dimension.setTwoDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());
            stringItemsSketchHashMap.get("threeByte").getItemsSketch().update(Dimension.setThreeDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());
            stringItemsSketchHashMap.get("fourByte").getItemsSketch().update(Dimension.setFourDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());
            stringItemsSketchHashMap.get("fiveByte").getItemsSketch().update(Dimension.setFiveDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());
            stringItemsSketchHashMap.get("sixByte").getItemsSketch().update(Dimension.setSixDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());
            stringItemsSketchHashMap.get("sevenByte").getItemsSketch().update(Dimension.setSevenDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());





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
