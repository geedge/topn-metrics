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
public class DatasketchForMetricsAggregate implements AggregateFunction<Entity, HashMap<String,DimensionItemsSketch>, HashMap<String,DimensionItemsSketch>> {

    private String key ;


    public DatasketchForMetricsAggregate(String key ){

        this.key = key;
    }


    @Override
    public HashMap<String, DimensionItemsSketch> createAccumulator() {
        return new HashMap<String, DimensionItemsSketch>(1048576);
    }

    @Override
    public HashMap<String, DimensionItemsSketch> add(Entity cnRecordLog, HashMap<String, DimensionItemsSketch> stringItemsSketchHashMap) {

//        String dimension = cnRecordLog.getCommon_client_ip();//维度
//        System.out.println(dimension);

        switch (key){
            case "oneSession":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> oneSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    oneSessionItemsSketch.update(Dimension.setOneDimension(cnRecordLog),cnRecordLog.getCommon_sessions());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                    collectionValue.setSource(cnRecordLog.getCommon_client_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setOneDimension(cnRecordLog),collectionValue);

                    DimensionItemsSketch oneSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.ONE, oneSessionItemsSketch,stringCollectionValueHashMap);


                    stringItemsSketchHashMap.put("oneSession", oneSessionDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("oneSession").getItemsSketch().update(Dimension.setOneDimension(cnRecordLog),cnRecordLog.getCommon_sessions());

                    CollectionValue oneSession = stringItemsSketchHashMap.get("oneSession").getStringCollectionValueHashMap().get(Dimension.setOneDimension(cnRecordLog));//从key获取集合

                    if (oneSession==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                        collectionValue.setSource(cnRecordLog.getCommon_client_ip());

                        stringItemsSketchHashMap.get("oneSession").getStringCollectionValueHashMap().put(Dimension.setOneDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        oneSession.getCollectionValue(oneSession,cnRecordLog);

                        stringItemsSketchHashMap.get("oneSession").getStringCollectionValueHashMap().put(Dimension.setOneDimension(cnRecordLog),oneSession);


                    }


                }

                break;
            case "twoSession":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> twoSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    twoSessionItemsSketch.update(Dimension.setTwoDimension(cnRecordLog),cnRecordLog.getCommon_sessions());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                    collectionValue.setSource(cnRecordLog.getCommon_server_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setTwoDimension(cnRecordLog),collectionValue);


                    DimensionItemsSketch twoSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.TWO,twoSessionItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("twoSession", twoSessionDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("twoSession").getItemsSketch().update(Dimension.setTwoDimension(cnRecordLog),cnRecordLog.getCommon_sessions());

                    CollectionValue twoSession = stringItemsSketchHashMap.get("twoSession").getStringCollectionValueHashMap().get(Dimension.setTwoDimension(cnRecordLog));


                    if (twoSession==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                        collectionValue.setSource(cnRecordLog.getCommon_server_ip());

                        stringItemsSketchHashMap.get("twoSession").getStringCollectionValueHashMap().put(Dimension.setTwoDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        twoSession.getCollectionValue(twoSession,cnRecordLog);

                        stringItemsSketchHashMap.get("twoSession").getStringCollectionValueHashMap().put(Dimension.setTwoDimension(cnRecordLog),twoSession);


                    }





                }
                break;
            case "threeSession":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> threeSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    threeSessionItemsSketch.update(Dimension.setThreeDimension(cnRecordLog),cnRecordLog.getCommon_sessions());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                    collectionValue.setSource(cnRecordLog.getCommon_internal_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setThreeDimension(cnRecordLog),collectionValue);



                    DimensionItemsSketch threeSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.THREE,threeSessionItemsSketch,stringCollectionValueHashMap);


                    stringItemsSketchHashMap.put("threeSession", threeSessionDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("threeSession").getItemsSketch().update(Dimension.setThreeDimension(cnRecordLog),cnRecordLog.getCommon_sessions());


                    CollectionValue threeSession = stringItemsSketchHashMap.get("threeSession").getStringCollectionValueHashMap().get(Dimension.setThreeDimension(cnRecordLog));


                    if (threeSession==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                        collectionValue.setSource(cnRecordLog.getCommon_server_ip());

                        stringItemsSketchHashMap.get("threeSession").getStringCollectionValueHashMap().put(Dimension.setThreeDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        threeSession.getCollectionValue(threeSession,cnRecordLog);

                        stringItemsSketchHashMap.get("threeSession").getStringCollectionValueHashMap().put(Dimension.setThreeDimension(cnRecordLog),threeSession);


                    }





                }
                break;
            case "fourSession":

                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> fourSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    fourSessionItemsSketch.update(Dimension.setFourDimension(cnRecordLog),cnRecordLog.getCommon_sessions());


                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                    collectionValue.setSource(cnRecordLog.getCommon_external_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setFourDimension(cnRecordLog),collectionValue);


                    DimensionItemsSketch fourSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.FOUR,fourSessionItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("fourSession", fourSessionDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("fourSession").getItemsSketch().update(Dimension.setFourDimension(cnRecordLog),cnRecordLog.getCommon_sessions());



                    CollectionValue fourSession = stringItemsSketchHashMap.get("fourSession").getStringCollectionValueHashMap().get(Dimension.setFourDimension(cnRecordLog));


                    if (fourSession==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                        collectionValue.setSource(cnRecordLog.getCommon_external_ip());

                        stringItemsSketchHashMap.get("fourSession").getStringCollectionValueHashMap().put(Dimension.setFourDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        fourSession.getCollectionValue(fourSession,cnRecordLog);

                        stringItemsSketchHashMap.get("fourSession").getStringCollectionValueHashMap().put(Dimension.setFourDimension(cnRecordLog),fourSession);


                    }





                }
                break;
            case "fiveSession":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> fiveSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    fiveSessionItemsSketch.update(Dimension.setFiveDimension(cnRecordLog),cnRecordLog.getCommon_sessions());


                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                    collectionValue.setSource(cnRecordLog.getHttp_domain());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setFiveDimension(cnRecordLog),collectionValue);




                    DimensionItemsSketch fiveSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.FIVE,fiveSessionItemsSketch,stringCollectionValueHashMap);


                    stringItemsSketchHashMap.put("fiveSession", fiveSessionDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("fiveSession").getItemsSketch().update(Dimension.setFiveDimension(cnRecordLog),cnRecordLog.getCommon_sessions());


                    CollectionValue fiveSession = stringItemsSketchHashMap.get("fiveSession").getStringCollectionValueHashMap().get(Dimension.setFiveDimension(cnRecordLog));


                    if (fiveSession==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                        collectionValue.setSource(cnRecordLog.getHttp_domain());

                        stringItemsSketchHashMap.get("fiveSession").getStringCollectionValueHashMap().put(Dimension.setFiveDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        fiveSession.getCollectionValue(fiveSession,cnRecordLog);

                        stringItemsSketchHashMap.get("fiveSession").getStringCollectionValueHashMap().put(Dimension.setFiveDimension(cnRecordLog),fiveSession);


                    }




                }
                break;
            case "sixSession":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> sixSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    sixSessionItemsSketch.update(Dimension.setSixDimension(cnRecordLog),cnRecordLog.getCommon_sessions());


                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                    collectionValue.setSource(cnRecordLog.getCommon_subscriber_id());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setSixDimension(cnRecordLog),collectionValue);



                    DimensionItemsSketch sixSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.SIX,sixSessionItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("sixSession", sixSessionDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("sixSession").getItemsSketch().update(Dimension.setSixDimension(cnRecordLog),cnRecordLog.getCommon_sessions());


                    CollectionValue sixSession = stringItemsSketchHashMap.get("sixSession").getStringCollectionValueHashMap().get(Dimension.setSixDimension(cnRecordLog));


                    if (sixSession==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                        collectionValue.setSource(cnRecordLog.getCommon_subscriber_id());

                        stringItemsSketchHashMap.get("sixSession").getStringCollectionValueHashMap().put(Dimension.setSixDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        sixSession.getCollectionValue(sixSession,cnRecordLog);

                        stringItemsSketchHashMap.get("sixSession").getStringCollectionValueHashMap().put(Dimension.setSixDimension(cnRecordLog),sixSession);


                    }







                }
                break;
            case "sevenSession":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> sevenSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    sevenSessionItemsSketch.update(Dimension.setSevenDimension(cnRecordLog),cnRecordLog.getCommon_sessions());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                    collectionValue.setSource(cnRecordLog.getCommon_app_label());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setSevenDimension(cnRecordLog),collectionValue);




                    DimensionItemsSketch sevenSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.SEVEN,sevenSessionItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("sevenSession", sevenSessionDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("sevenSession").getItemsSketch().update(Dimension.setSevenDimension(cnRecordLog),cnRecordLog.getCommon_sessions());


                    CollectionValue sevenSession = stringItemsSketchHashMap.get("sevenSession").getStringCollectionValueHashMap().get(Dimension.setSevenDimension(cnRecordLog));


                    if (sevenSession==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"sessions");
                        collectionValue.setSource(cnRecordLog.getCommon_app_label());

                        stringItemsSketchHashMap.get("sevenSession").getStringCollectionValueHashMap().put(Dimension.setSevenDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        sevenSession.getCollectionValue(sevenSession,cnRecordLog);

                        stringItemsSketchHashMap.get("sevenSession").getStringCollectionValueHashMap().put(Dimension.setSevenDimension(cnRecordLog),sevenSession);


                    }




                }
                break;
            case "onePkt":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> onePktItemsSketch = new ItemsSketch<>(1048576);

                    onePktItemsSketch.update(Dimension.setOneDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                    collectionValue.setSource(cnRecordLog.getCommon_client_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setOneDimension(cnRecordLog),collectionValue);



                    DimensionItemsSketch onePktDimensionItemsSketch = new DimensionItemsSketch(Dimension.ONE,onePktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("onePkt",onePktDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("onePkt").getItemsSketch().update(Dimension.setOneDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());

                    CollectionValue onePkt = stringItemsSketchHashMap.get("onePkt").getStringCollectionValueHashMap().get(Dimension.setOneDimension(cnRecordLog));//从key获取集合

                    if (onePkt==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                        collectionValue.setSource(cnRecordLog.getCommon_client_ip());

                        stringItemsSketchHashMap.get("onePkt").getStringCollectionValueHashMap().put(Dimension.setOneDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        onePkt.getCollectionValue(onePkt,cnRecordLog);

                        stringItemsSketchHashMap.get("onePkt").getStringCollectionValueHashMap().put(Dimension.setOneDimension(cnRecordLog),onePkt);


                    }




                }
                break;
            case "twoPkt":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> twoPktItemsSketch = new ItemsSketch<>(1048576);

                    twoPktItemsSketch.update(Dimension.setTwoDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                    collectionValue.setSource(cnRecordLog.getCommon_server_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setTwoDimension(cnRecordLog),collectionValue);




                    DimensionItemsSketch twoPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.TWO,twoPktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("twoPkt",twoPktDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("twoPkt").getItemsSketch().update(Dimension.setTwoDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());


                    CollectionValue twoPkt = stringItemsSketchHashMap.get("twoPkt").getStringCollectionValueHashMap().get(Dimension.setTwoDimension(cnRecordLog));


                    if (twoPkt==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                        collectionValue.setSource(cnRecordLog.getCommon_server_ip());

                        stringItemsSketchHashMap.get("twoPkt").getStringCollectionValueHashMap().put(Dimension.setTwoDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        twoPkt.getCollectionValue(twoPkt,cnRecordLog);

                        stringItemsSketchHashMap.get("twoPkt").getStringCollectionValueHashMap().put(Dimension.setTwoDimension(cnRecordLog),twoPkt);


                    }




                }
                break;
            case "threePkt":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> threePktItemsSketch = new ItemsSketch<>(1048576);

                    threePktItemsSketch.update(Dimension.setThreeDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                    collectionValue.setSource(cnRecordLog.getCommon_internal_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setThreeDimension(cnRecordLog),collectionValue);






                    DimensionItemsSketch threePktDimensionItemsSketch = new DimensionItemsSketch(Dimension.THREE,threePktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("threePkt",threePktDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("threePkt").getItemsSketch().update(Dimension.setThreeDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());


                    CollectionValue threePkt = stringItemsSketchHashMap.get("threePkt").getStringCollectionValueHashMap().get(Dimension.setThreeDimension(cnRecordLog));


                    if (threePkt==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                        collectionValue.setSource(cnRecordLog.getCommon_server_ip());

                        stringItemsSketchHashMap.get("threePkt").getStringCollectionValueHashMap().put(Dimension.setThreeDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        threePkt.getCollectionValue(threePkt,cnRecordLog);

                        stringItemsSketchHashMap.get("threePkt").getStringCollectionValueHashMap().put(Dimension.setThreeDimension(cnRecordLog),threePkt);


                    }





                }
                break;
            case "fourPkt":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> fourPktItemsSketch = new ItemsSketch<>(1048576);

                    fourPktItemsSketch.update(Dimension.setFourDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());


                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                    collectionValue.setSource(cnRecordLog.getCommon_external_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setFourDimension(cnRecordLog),collectionValue);



                    DimensionItemsSketch fourPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.FOUR,fourPktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("fourPkt",fourPktDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("fourPkt").getItemsSketch().update(Dimension.setFourDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());



                    CollectionValue fourPkt = stringItemsSketchHashMap.get("fourPkt").getStringCollectionValueHashMap().get(Dimension.setFourDimension(cnRecordLog));


                    if (fourPkt==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                        collectionValue.setSource(cnRecordLog.getCommon_external_ip());

                        stringItemsSketchHashMap.get("fourPkt").getStringCollectionValueHashMap().put(Dimension.setFourDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        fourPkt.getCollectionValue(fourPkt,cnRecordLog);

                        stringItemsSketchHashMap.get("fourPkt").getStringCollectionValueHashMap().put(Dimension.setFourDimension(cnRecordLog),fourPkt);


                    }






                }
                break;
            case "fivePkt":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> fivePktItemsSketch = new ItemsSketch<>(1048576);

                    fivePktItemsSketch.update(Dimension.setFiveDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                    collectionValue.setSource(cnRecordLog.getHttp_domain());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setFiveDimension(cnRecordLog),collectionValue);





                    DimensionItemsSketch fivePktDimensionItemsSketch = new DimensionItemsSketch(Dimension.FIVE,fivePktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("fivePkt",fivePktDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("fivePkt").getItemsSketch().update(Dimension.setFiveDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());

                    CollectionValue fivePkt = stringItemsSketchHashMap.get("fivePkt").getStringCollectionValueHashMap().get(Dimension.setFiveDimension(cnRecordLog));


                    if (fivePkt==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                        collectionValue.setSource(cnRecordLog.getHttp_domain());

                        stringItemsSketchHashMap.get("fivePkt").getStringCollectionValueHashMap().put(Dimension.setFiveDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        fivePkt.getCollectionValue(fivePkt,cnRecordLog);

                        stringItemsSketchHashMap.get("fivePkt").getStringCollectionValueHashMap().put(Dimension.setFiveDimension(cnRecordLog),fivePkt);


                    }





                }
                break;
            case "sixPkt":
                if(stringItemsSketchHashMap.isEmpty()) {


                    ItemsSketch<String> sixPktItemsSketch = new ItemsSketch<>(1048576);

                    sixPktItemsSketch.update(Dimension.setSixDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                    collectionValue.setSource(cnRecordLog.getCommon_subscriber_id());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setSixDimension(cnRecordLog),collectionValue);





                    DimensionItemsSketch sixPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.SIX,sixPktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("sixPkt",sixPktDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("sixPkt").getItemsSketch().update(Dimension.setSixDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());

                    CollectionValue sixPkt = stringItemsSketchHashMap.get("sixPkt").getStringCollectionValueHashMap().get(Dimension.setSixDimension(cnRecordLog));


                    if (sixPkt==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                        collectionValue.setSource(cnRecordLog.getCommon_subscriber_id());

                        stringItemsSketchHashMap.get("sixPkt").getStringCollectionValueHashMap().put(Dimension.setSixDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        sixPkt.getCollectionValue(sixPkt,cnRecordLog);

                        stringItemsSketchHashMap.get("sixPkt").getStringCollectionValueHashMap().put(Dimension.setSixDimension(cnRecordLog),sixPkt);


                    }





                }
                break;
            case "sevenPkt":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> sevenPktItemsSketch = new ItemsSketch<>(1048576);

                    sevenPktItemsSketch.update(Dimension.setSevenDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                    collectionValue.setSource(cnRecordLog.getCommon_app_label());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setSevenDimension(cnRecordLog),collectionValue);





                    DimensionItemsSketch sevenPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.SEVEN,sevenPktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("sevenPkt",sevenPktDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("sevenPkt").getItemsSketch().update(Dimension.setSevenDimension(cnRecordLog),cnRecordLog.getCommon_c2s_pkt_num()+cnRecordLog.getCommon_s2c_pkt_num());

                    CollectionValue sevenPkt = stringItemsSketchHashMap.get("sevenPkt").getStringCollectionValueHashMap().get(Dimension.setSevenDimension(cnRecordLog));


                    if (sevenPkt==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"packets");
                        collectionValue.setSource(cnRecordLog.getCommon_app_label());

                        stringItemsSketchHashMap.get("sevenPkt").getStringCollectionValueHashMap().put(Dimension.setSevenDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        sevenPkt.getCollectionValue(sevenPkt,cnRecordLog);

                        stringItemsSketchHashMap.get("sevenPkt").getStringCollectionValueHashMap().put(Dimension.setSevenDimension(cnRecordLog),sevenPkt);


                    }




                }
                break;
            case "oneByte":
                if(stringItemsSketchHashMap.isEmpty()) {


                    ItemsSketch<String> oneByteItemsSketch = new ItemsSketch<>(1048576);


                    oneByteItemsSketch.update(Dimension.setOneDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                    collectionValue.setSource(cnRecordLog.getCommon_client_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setOneDimension(cnRecordLog),collectionValue);





                    DimensionItemsSketch oneByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.ONE,oneByteItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("oneByte",oneByteDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("oneByte").getItemsSketch().update(Dimension.setOneDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());


                    CollectionValue oneByte = stringItemsSketchHashMap.get("oneByte").getStringCollectionValueHashMap().get(Dimension.setOneDimension(cnRecordLog));//从key获取集合

                    if (oneByte==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                        collectionValue.setSource(cnRecordLog.getCommon_client_ip());

                        stringItemsSketchHashMap.get("oneByte").getStringCollectionValueHashMap().put(Dimension.setOneDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        oneByte.getCollectionValue(oneByte,cnRecordLog);

                        stringItemsSketchHashMap.get("oneByte").getStringCollectionValueHashMap().put(Dimension.setOneDimension(cnRecordLog),oneByte);


                    }



                }
                break;
            case "twoByte":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> twoByteItemsSketch = new ItemsSketch<>(1048576);

                    twoByteItemsSketch.update(Dimension.setTwoDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                    collectionValue.setSource(cnRecordLog.getCommon_server_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setTwoDimension(cnRecordLog),collectionValue);





                    DimensionItemsSketch twoByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.TWO,twoByteItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("twoByte",twoByteDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("twoByte").getItemsSketch().update(Dimension.setTwoDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());

                    CollectionValue twoByte = stringItemsSketchHashMap.get("twoByte").getStringCollectionValueHashMap().get(Dimension.setTwoDimension(cnRecordLog));


                    if (twoByte==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                        collectionValue.setSource(cnRecordLog.getCommon_server_ip());

                        stringItemsSketchHashMap.get("twoByte").getStringCollectionValueHashMap().put(Dimension.setTwoDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        twoByte.getCollectionValue(twoByte,cnRecordLog);

                        stringItemsSketchHashMap.get("twoByte").getStringCollectionValueHashMap().put(Dimension.setTwoDimension(cnRecordLog),twoByte);


                    }



                }
                break;
            case "threeByte":
                if(stringItemsSketchHashMap.isEmpty()) {


                    ItemsSketch<String> threeByteItemsSketch = new ItemsSketch<>(1048576);

                    threeByteItemsSketch.update(Dimension.setThreeDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                    collectionValue.setSource(cnRecordLog.getCommon_internal_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setThreeDimension(cnRecordLog),collectionValue);






                    DimensionItemsSketch threeByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.THREE,threeByteItemsSketch,stringCollectionValueHashMap);


                    stringItemsSketchHashMap.put("threeByte",threeByteDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("threeByte").getItemsSketch().update(Dimension.setThreeDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());


                    CollectionValue threeByte = stringItemsSketchHashMap.get("threeByte").getStringCollectionValueHashMap().get(Dimension.setThreeDimension(cnRecordLog));


                    if (threeByte==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                        collectionValue.setSource(cnRecordLog.getCommon_server_ip());

                        stringItemsSketchHashMap.get("threeByte").getStringCollectionValueHashMap().put(Dimension.setThreeDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        threeByte.getCollectionValue(threeByte,cnRecordLog);

                        stringItemsSketchHashMap.get("threeByte").getStringCollectionValueHashMap().put(Dimension.setThreeDimension(cnRecordLog),threeByte);


                    }





                }
                break;
            case "fourByte":
                if(stringItemsSketchHashMap.isEmpty()) {



                    ItemsSketch<String> fourByteItemsSketch = new ItemsSketch<>(1048576);

                    fourByteItemsSketch.update(Dimension.setFourDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());


                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                    collectionValue.setSource(cnRecordLog.getCommon_external_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setFourDimension(cnRecordLog),collectionValue);





                    DimensionItemsSketch fourByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.FOUR,fourByteItemsSketch,stringCollectionValueHashMap);



                    stringItemsSketchHashMap.put("fourByte",fourByteDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("fourByte").getItemsSketch().update(Dimension.setFourDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());

                    CollectionValue fourByte = stringItemsSketchHashMap.get("fourByte").getStringCollectionValueHashMap().get(Dimension.setFourDimension(cnRecordLog));


                    if (fourByte==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                        collectionValue.setSource(cnRecordLog.getCommon_external_ip());

                        stringItemsSketchHashMap.get("fourByte").getStringCollectionValueHashMap().put(Dimension.setFourDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        fourByte.getCollectionValue(fourByte,cnRecordLog);

                        stringItemsSketchHashMap.get("fourByte").getStringCollectionValueHashMap().put(Dimension.setFourDimension(cnRecordLog),fourByte);


                    }






                }

                break;
            case "fiveByte":
                if(stringItemsSketchHashMap.isEmpty()) {


                    ItemsSketch<String> fiveByteItemsSketch = new ItemsSketch<>(1048576);

                    fiveByteItemsSketch.update(Dimension.setFiveDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());

                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                    collectionValue.setSource(cnRecordLog.getHttp_domain());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setFiveDimension(cnRecordLog),collectionValue);






                    DimensionItemsSketch fiveByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.FIVE,fiveByteItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("fiveByte",fiveByteDimensionItemsSketch);

                }else {
                    stringItemsSketchHashMap.get("fiveByte").getItemsSketch().update(Dimension.setFiveDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());

                    CollectionValue fiveByte = stringItemsSketchHashMap.get("fiveByte").getStringCollectionValueHashMap().get(Dimension.setFiveDimension(cnRecordLog));


                    if (fiveByte==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                        collectionValue.setSource(cnRecordLog.getHttp_domain());

                        stringItemsSketchHashMap.get("fiveByte").getStringCollectionValueHashMap().put(Dimension.setFiveDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        fiveByte.getCollectionValue(fiveByte,cnRecordLog);

                        stringItemsSketchHashMap.get("fiveByte").getStringCollectionValueHashMap().put(Dimension.setFiveDimension(cnRecordLog),fiveByte);


                    }





                }

                break;
            case "sixByte":
                if(stringItemsSketchHashMap.isEmpty()) {


                    ItemsSketch<String> sixByteItemsSketch = new ItemsSketch<>(1048576);

                    sixByteItemsSketch.update(Dimension.setSixDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());


                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                    collectionValue.setSource(cnRecordLog.getCommon_subscriber_id());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setSixDimension(cnRecordLog),collectionValue);





                    DimensionItemsSketch sixByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.SIX,sixByteItemsSketch,stringCollectionValueHashMap);


                    stringItemsSketchHashMap.put("sixByte",sixByteDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("sixByte").getItemsSketch().update(Dimension.setSixDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());

                    CollectionValue sixByte = stringItemsSketchHashMap.get("sixByte").getStringCollectionValueHashMap().get(Dimension.setSixDimension(cnRecordLog));


                    if (sixByte==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                        collectionValue.setSource(cnRecordLog.getCommon_subscriber_id());

                        stringItemsSketchHashMap.get("sixByte").getStringCollectionValueHashMap().put(Dimension.setSixDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        sixByte.getCollectionValue(sixByte,cnRecordLog);

                        stringItemsSketchHashMap.get("sixByte").getStringCollectionValueHashMap().put(Dimension.setSixDimension(cnRecordLog),sixByte);


                    }






                }

                break;
            case "sevenByte":
                if(stringItemsSketchHashMap.isEmpty()) {


                    ItemsSketch<String> sevenByteItemsSketch = new ItemsSketch<>(1048576);


                    sevenByteItemsSketch.update(Dimension.setSevenDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());


                    CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                    collectionValue.setSource(cnRecordLog.getCommon_app_label());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setSevenDimension(cnRecordLog),collectionValue);





                    DimensionItemsSketch sevenByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.SEVEN,sevenByteItemsSketch,stringCollectionValueHashMap);


                    stringItemsSketchHashMap.put("sevenByte",sevenByteDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("sevenByte").getItemsSketch().update(Dimension.setSevenDimension(cnRecordLog),cnRecordLog.getCommon_c2s_byte_num()+cnRecordLog.getCommon_s2c_byte_num());

                    CollectionValue sevenByte = stringItemsSketchHashMap.get("sevenByte").getStringCollectionValueHashMap().get(Dimension.setSevenDimension(cnRecordLog));


                    if (sevenByte==null){

                        CollectionValue collectionValue = new CollectionValue(cnRecordLog,"bytes");
                        collectionValue.setSource(cnRecordLog.getCommon_app_label());

                        stringItemsSketchHashMap.get("sevenByte").getStringCollectionValueHashMap().put(Dimension.setSevenDimension(cnRecordLog),collectionValue);

                    }else {//做加和

                        sevenByte.getCollectionValue(sevenByte,cnRecordLog);

                        stringItemsSketchHashMap.get("sevenByte").getStringCollectionValueHashMap().put(Dimension.setSevenDimension(cnRecordLog),sevenByte);


                    }



                }
                break;


        }





        return stringItemsSketchHashMap;
    }

    @Override
    public HashMap<String, DimensionItemsSketch> getResult(HashMap<String, DimensionItemsSketch> stringItemsSketchHashMap) {


        return stringItemsSketchHashMap;
    }

    @Override
    public HashMap<String, DimensionItemsSketch> merge(HashMap<String, DimensionItemsSketch> stringItemsSketchHashMap, HashMap<String, DimensionItemsSketch> acc1) {



        return null;
    }
}
