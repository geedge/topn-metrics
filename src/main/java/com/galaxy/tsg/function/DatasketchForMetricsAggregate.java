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
    public HashMap<String, DimensionItemsSketch> add(Entity entity, HashMap<String, DimensionItemsSketch> stringItemsSketchHashMap) {

//        String dimension = cnRecordLog.getCommon_client_ip();//维度
//        System.out.println(dimension);

        switch (key){
            case "clientIpSession":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> clientIpSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    clientIpSessionItemsSketch.update(Dimension.setClientIpDimension(entity),entity.getCommon_sessions());

                    CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                    collectionValue.setSource(entity.getCommon_client_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setClientIpDimension(entity),collectionValue);

                    DimensionItemsSketch clientIpSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.CLIENTIP, clientIpSessionItemsSketch,stringCollectionValueHashMap);


                    stringItemsSketchHashMap.put("clientIpSession", clientIpSessionDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("clientIpSession").getItemsSketch().update(Dimension.setClientIpDimension(entity),entity.getCommon_sessions());

                    CollectionValue clientIpSession = stringItemsSketchHashMap.get("clientIpSession").getStringCollectionValueHashMap().get(Dimension.setClientIpDimension(entity));//从key获取集合

                    if (clientIpSession==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                        collectionValue.setSource(entity.getCommon_client_ip());

                        stringItemsSketchHashMap.get("clientIpSession").getStringCollectionValueHashMap().put(Dimension.setClientIpDimension(entity),collectionValue);

                    }else {//做加和

                        clientIpSession.getCollectionValue(clientIpSession,entity);

                        stringItemsSketchHashMap.get("clientIpSession").getStringCollectionValueHashMap().put(Dimension.setClientIpDimension(entity),clientIpSession);


                    }


                }

                break;
            case "serverIpSession":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> serverIpSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    serverIpSessionItemsSketch.update(Dimension.setServerIpDimension(entity),entity.getCommon_sessions());

                    CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                    collectionValue.setSource(entity.getCommon_server_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setServerIpDimension(entity),collectionValue);


                    DimensionItemsSketch serverIpSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.SERVERIP,serverIpSessionItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("serverIpSession", serverIpSessionDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("serverIpSession").getItemsSketch().update(Dimension.setServerIpDimension(entity),entity.getCommon_sessions());

                    CollectionValue serverIpSession = stringItemsSketchHashMap.get("serverIpSession").getStringCollectionValueHashMap().get(Dimension.setServerIpDimension(entity));


                    if (serverIpSession==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                        collectionValue.setSource(entity.getCommon_server_ip());

                        stringItemsSketchHashMap.get("serverIpSession").getStringCollectionValueHashMap().put(Dimension.setServerIpDimension(entity),collectionValue);

                    }else {//做加和

                        serverIpSession.getCollectionValue(serverIpSession,entity);

                        stringItemsSketchHashMap.get("serverIpSession").getStringCollectionValueHashMap().put(Dimension.setServerIpDimension(entity),serverIpSession);


                    }





                }
                break;
            case "internalIpSession":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> internalIpSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    internalIpSessionItemsSketch.update(Dimension.setInternalIpDimension(entity),entity.getCommon_sessions());

                    CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                    collectionValue.setSource(entity.getCommon_internal_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setInternalIpDimension(entity),collectionValue);



                    DimensionItemsSketch internalIpSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.INTERNALIP,internalIpSessionItemsSketch,stringCollectionValueHashMap);


                    stringItemsSketchHashMap.put("internalIpSession", internalIpSessionDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("internalIpSession").getItemsSketch().update(Dimension.setInternalIpDimension(entity),entity.getCommon_sessions());


                    CollectionValue internalIpSession = stringItemsSketchHashMap.get("internalIpSession").getStringCollectionValueHashMap().get(Dimension.setInternalIpDimension(entity));


                    if (internalIpSession==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                        collectionValue.setSource(entity.getCommon_server_ip());

                        stringItemsSketchHashMap.get("internalIpSession").getStringCollectionValueHashMap().put(Dimension.setInternalIpDimension(entity),collectionValue);

                    }else {//做加和

                        internalIpSession.getCollectionValue(internalIpSession,entity);

                        stringItemsSketchHashMap.get("internalIpSession").getStringCollectionValueHashMap().put(Dimension.setInternalIpDimension(entity),internalIpSession);


                    }





                }
                break;
            case "externalIpSession":

                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> externalIpSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    externalIpSessionItemsSketch.update(Dimension.setExternalIpDimension(entity),entity.getCommon_sessions());


                    CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                    collectionValue.setSource(entity.getCommon_external_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setExternalIpDimension(entity),collectionValue);


                    DimensionItemsSketch externalIpSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.EXTERNALIP,externalIpSessionItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("externalIpSession", externalIpSessionDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("externalIpSession").getItemsSketch().update(Dimension.setExternalIpDimension(entity),entity.getCommon_sessions());



                    CollectionValue externalIpSession = stringItemsSketchHashMap.get("externalIpSession").getStringCollectionValueHashMap().get(Dimension.setExternalIpDimension(entity));


                    if (externalIpSession==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                        collectionValue.setSource(entity.getCommon_external_ip());

                        stringItemsSketchHashMap.get("externalIpSession").getStringCollectionValueHashMap().put(Dimension.setExternalIpDimension(entity),collectionValue);

                    }else {//做加和

                        externalIpSession.getCollectionValue(externalIpSession,entity);

                        stringItemsSketchHashMap.get("externalIpSession").getStringCollectionValueHashMap().put(Dimension.setExternalIpDimension(entity),externalIpSession);


                    }





                }
                break;
            case "domainSession":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> domainSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    domainSessionItemsSketch.update(Dimension.setDomainDimension(entity),entity.getCommon_sessions());


                    CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                    collectionValue.setSource(entity.getHttp_domain());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setDomainDimension(entity),collectionValue);




                    DimensionItemsSketch domainSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.DOMAIN,domainSessionItemsSketch,stringCollectionValueHashMap);


                    stringItemsSketchHashMap.put("domainSession", domainSessionDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("domainSession").getItemsSketch().update(Dimension.setDomainDimension(entity),entity.getCommon_sessions());


                    CollectionValue domainSession = stringItemsSketchHashMap.get("domainSession").getStringCollectionValueHashMap().get(Dimension.setDomainDimension(entity));


                    if (domainSession==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                        collectionValue.setSource(entity.getHttp_domain());

                        stringItemsSketchHashMap.get("domainSession").getStringCollectionValueHashMap().put(Dimension.setDomainDimension(entity),collectionValue);

                    }else {//做加和

                        domainSession.getCollectionValue(domainSession,entity);

                        stringItemsSketchHashMap.get("domainSession").getStringCollectionValueHashMap().put(Dimension.setDomainDimension(entity),domainSession);


                    }




                }
                break;
            case "subscriberIdSession":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> subscriberIdSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    subscriberIdSessionItemsSketch.update(Dimension.setSubscriberIdDimension(entity),entity.getCommon_sessions());


                    CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                    collectionValue.setSource(entity.getCommon_subscriber_id());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setSubscriberIdDimension(entity),collectionValue);



                    DimensionItemsSketch subscriberIdSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.SUBSCRIBERID,subscriberIdSessionItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("subscriberIdSession", subscriberIdSessionDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("subscriberIdSession").getItemsSketch().update(Dimension.setSubscriberIdDimension(entity),entity.getCommon_sessions());


                    CollectionValue subscriberIdSession = stringItemsSketchHashMap.get("subscriberIdSession").getStringCollectionValueHashMap().get(Dimension.setSubscriberIdDimension(entity));


                    if (subscriberIdSession==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                        collectionValue.setSource(entity.getCommon_subscriber_id());

                        stringItemsSketchHashMap.get("subscriberIdSession").getStringCollectionValueHashMap().put(Dimension.setSubscriberIdDimension(entity),collectionValue);

                    }else {//做加和

                        subscriberIdSession.getCollectionValue(subscriberIdSession,entity);

                        stringItemsSketchHashMap.get("subscriberIdSession").getStringCollectionValueHashMap().put(Dimension.setSubscriberIdDimension(entity),subscriberIdSession);


                    }







                }
                break;
            case "appLabelSession":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> appLabelSessionItemsSketch = new ItemsSketch<>(1048576);//新建

                    appLabelSessionItemsSketch.update(Dimension.setAppLabelDimension(entity),entity.getCommon_sessions());

                    CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                    collectionValue.setSource(entity.getCommon_app_label());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setAppLabelDimension(entity),collectionValue);




                    DimensionItemsSketch appLabelSessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.APPLABEL,appLabelSessionItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("appLabelSession", appLabelSessionDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("appLabelSession").getItemsSketch().update(Dimension.setAppLabelDimension(entity),entity.getCommon_sessions());


                    CollectionValue appLabelSession = stringItemsSketchHashMap.get("appLabelSession").getStringCollectionValueHashMap().get(Dimension.setAppLabelDimension(entity));


                    if (appLabelSession==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"sessions");
                        collectionValue.setSource(entity.getCommon_app_label());

                        stringItemsSketchHashMap.get("appLabelSession").getStringCollectionValueHashMap().put(Dimension.setAppLabelDimension(entity),collectionValue);

                    }else {//做加和

                        appLabelSession.getCollectionValue(appLabelSession,entity);

                        stringItemsSketchHashMap.get("appLabelSession").getStringCollectionValueHashMap().put(Dimension.setAppLabelDimension(entity),appLabelSession);


                    }




                }
                break;
            case "clientIpPkt":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> clientIpPktItemsSketch = new ItemsSketch<>(1048576);

                    clientIpPktItemsSketch.update(Dimension.setClientIpDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());

                    CollectionValue collectionValue = new CollectionValue(entity,"packets");
                    collectionValue.setSource(entity.getCommon_client_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setClientIpDimension(entity),collectionValue);



                    DimensionItemsSketch clientIpPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.CLIENTIP,clientIpPktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("clientIpPkt",clientIpPktDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("clientIpPkt").getItemsSketch().update(Dimension.setClientIpDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());

                    CollectionValue clientIpPkt = stringItemsSketchHashMap.get("clientIpPkt").getStringCollectionValueHashMap().get(Dimension.setClientIpDimension(entity));//从key获取集合

                    if (clientIpPkt==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"packets");
                        collectionValue.setSource(entity.getCommon_client_ip());

                        stringItemsSketchHashMap.get("clientIpPkt").getStringCollectionValueHashMap().put(Dimension.setClientIpDimension(entity),collectionValue);

                    }else {//做加和

                        clientIpPkt.getCollectionValue(clientIpPkt,entity);

                        stringItemsSketchHashMap.get("clientIpPkt").getStringCollectionValueHashMap().put(Dimension.setClientIpDimension(entity),clientIpPkt);


                    }




                }
                break;
            case "serverIpPkt":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> serverIpPktItemsSketch = new ItemsSketch<>(1048576);

                    serverIpPktItemsSketch.update(Dimension.setServerIpDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());

                    CollectionValue collectionValue = new CollectionValue(entity,"packets");
                    collectionValue.setSource(entity.getCommon_server_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setServerIpDimension(entity),collectionValue);




                    DimensionItemsSketch serverIpPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.SERVERIP,serverIpPktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("serverIpPkt",serverIpPktDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("serverIpPkt").getItemsSketch().update(Dimension.setServerIpDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());


                    CollectionValue serverIpPkt = stringItemsSketchHashMap.get("serverIpPkt").getStringCollectionValueHashMap().get(Dimension.setServerIpDimension(entity));


                    if (serverIpPkt==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"packets");
                        collectionValue.setSource(entity.getCommon_server_ip());

                        stringItemsSketchHashMap.get("serverIpPkt").getStringCollectionValueHashMap().put(Dimension.setServerIpDimension(entity),collectionValue);

                    }else {//做加和

                        serverIpPkt.getCollectionValue(serverIpPkt,entity);

                        stringItemsSketchHashMap.get("serverIpPkt").getStringCollectionValueHashMap().put(Dimension.setServerIpDimension(entity),serverIpPkt);


                    }




                }
                break;
            case "internalIpPkt":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> internalIpPktItemsSketch = new ItemsSketch<>(1048576);

                    internalIpPktItemsSketch.update(Dimension.setInternalIpDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());

                    CollectionValue collectionValue = new CollectionValue(entity,"packets");
                    collectionValue.setSource(entity.getCommon_internal_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setInternalIpDimension(entity),collectionValue);






                    DimensionItemsSketch internalIpPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.INTERNALIP,internalIpPktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("internalIpPkt",internalIpPktDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("internalIpPkt").getItemsSketch().update(Dimension.setInternalIpDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());


                    CollectionValue internalIpPkt = stringItemsSketchHashMap.get("internalIpPkt").getStringCollectionValueHashMap().get(Dimension.setInternalIpDimension(entity));


                    if (internalIpPkt==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"packets");
                        collectionValue.setSource(entity.getCommon_server_ip());

                        stringItemsSketchHashMap.get("internalIpPkt").getStringCollectionValueHashMap().put(Dimension.setInternalIpDimension(entity),collectionValue);

                    }else {//做加和

                        internalIpPkt.getCollectionValue(internalIpPkt,entity);

                        stringItemsSketchHashMap.get("internalIpPkt").getStringCollectionValueHashMap().put(Dimension.setInternalIpDimension(entity),internalIpPkt);


                    }





                }
                break;
            case "externalIpPkt":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> externalIpPktItemsSketch = new ItemsSketch<>(1048576);

                    externalIpPktItemsSketch.update(Dimension.setExternalIpDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());


                    CollectionValue collectionValue = new CollectionValue(entity,"packets");
                    collectionValue.setSource(entity.getCommon_external_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setExternalIpDimension(entity),collectionValue);



                    DimensionItemsSketch externalIpPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.EXTERNALIP,externalIpPktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("externalIpPkt",externalIpPktDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("externalIpPkt").getItemsSketch().update(Dimension.setExternalIpDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());



                    CollectionValue externalIpPkt = stringItemsSketchHashMap.get("externalIpPkt").getStringCollectionValueHashMap().get(Dimension.setExternalIpDimension(entity));


                    if (externalIpPkt==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"packets");
                        collectionValue.setSource(entity.getCommon_external_ip());

                        stringItemsSketchHashMap.get("externalIpPkt").getStringCollectionValueHashMap().put(Dimension.setExternalIpDimension(entity),collectionValue);

                    }else {//做加和

                        externalIpPkt.getCollectionValue(externalIpPkt,entity);

                        stringItemsSketchHashMap.get("externalIpPkt").getStringCollectionValueHashMap().put(Dimension.setExternalIpDimension(entity),externalIpPkt);


                    }






                }
                break;
            case "domainPkt":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> domainPktItemsSketch = new ItemsSketch<>(1048576);

                    domainPktItemsSketch.update(Dimension.setDomainDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());

                    CollectionValue collectionValue = new CollectionValue(entity,"packets");
                    collectionValue.setSource(entity.getHttp_domain());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setDomainDimension(entity),collectionValue);





                    DimensionItemsSketch domainPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.DOMAIN,domainPktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("domainPkt",domainPktDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("domainPkt").getItemsSketch().update(Dimension.setDomainDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());

                    CollectionValue domainPkt = stringItemsSketchHashMap.get("domainPkt").getStringCollectionValueHashMap().get(Dimension.setDomainDimension(entity));


                    if (domainPkt==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"packets");
                        collectionValue.setSource(entity.getHttp_domain());

                        stringItemsSketchHashMap.get("domainPkt").getStringCollectionValueHashMap().put(Dimension.setDomainDimension(entity),collectionValue);

                    }else {//做加和

                        domainPkt.getCollectionValue(domainPkt,entity);

                        stringItemsSketchHashMap.get("domainPkt").getStringCollectionValueHashMap().put(Dimension.setDomainDimension(entity),domainPkt);


                    }





                }
                break;
            case "subscriberIdPkt":
                if(stringItemsSketchHashMap.isEmpty()) {


                    ItemsSketch<String> subscriberIdPktItemsSketch = new ItemsSketch<>(1048576);

                    subscriberIdPktItemsSketch.update(Dimension.setSubscriberIdDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());

                    CollectionValue collectionValue = new CollectionValue(entity,"packets");
                    collectionValue.setSource(entity.getCommon_subscriber_id());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setSubscriberIdDimension(entity),collectionValue);





                    DimensionItemsSketch subscriberIdPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.SUBSCRIBERID,subscriberIdPktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("subscriberIdPkt",subscriberIdPktDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("subscriberIdPkt").getItemsSketch().update(Dimension.setSubscriberIdDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());

                    CollectionValue subscriberIdPkt = stringItemsSketchHashMap.get("subscriberIdPkt").getStringCollectionValueHashMap().get(Dimension.setSubscriberIdDimension(entity));


                    if (subscriberIdPkt==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"packets");
                        collectionValue.setSource(entity.getCommon_subscriber_id());

                        stringItemsSketchHashMap.get("subscriberIdPkt").getStringCollectionValueHashMap().put(Dimension.setSubscriberIdDimension(entity),collectionValue);

                    }else {//做加和

                        subscriberIdPkt.getCollectionValue(subscriberIdPkt,entity);

                        stringItemsSketchHashMap.get("subscriberIdPkt").getStringCollectionValueHashMap().put(Dimension.setSubscriberIdDimension(entity),subscriberIdPkt);


                    }





                }
                break;
            case "appLabelPkt":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> appLabelPktItemsSketch = new ItemsSketch<>(1048576);

                    appLabelPktItemsSketch.update(Dimension.setAppLabelDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());

                    CollectionValue collectionValue = new CollectionValue(entity,"packets");
                    collectionValue.setSource(entity.getCommon_app_label());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setAppLabelDimension(entity),collectionValue);





                    DimensionItemsSketch appLabelPktDimensionItemsSketch = new DimensionItemsSketch(Dimension.APPLABEL,appLabelPktItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("appLabelPkt",appLabelPktDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("appLabelPkt").getItemsSketch().update(Dimension.setAppLabelDimension(entity),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());

                    CollectionValue appLabelPkt = stringItemsSketchHashMap.get("appLabelPkt").getStringCollectionValueHashMap().get(Dimension.setAppLabelDimension(entity));


                    if (appLabelPkt==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"packets");
                        collectionValue.setSource(entity.getCommon_app_label());

                        stringItemsSketchHashMap.get("appLabelPkt").getStringCollectionValueHashMap().put(Dimension.setAppLabelDimension(entity),collectionValue);

                    }else {//做加和

                        appLabelPkt.getCollectionValue(appLabelPkt,entity);

                        stringItemsSketchHashMap.get("appLabelPkt").getStringCollectionValueHashMap().put(Dimension.setAppLabelDimension(entity),appLabelPkt);


                    }




                }
                break;
            case "clientIpByte":
                if(stringItemsSketchHashMap.isEmpty()) {


                    ItemsSketch<String> clientIpByteItemsSketch = new ItemsSketch<>(1048576);


                    clientIpByteItemsSketch.update(Dimension.setClientIpDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());

                    CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                    collectionValue.setSource(entity.getCommon_client_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setClientIpDimension(entity),collectionValue);





                    DimensionItemsSketch clientIpByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.CLIENTIP,clientIpByteItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("clientIpByte",clientIpByteDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("clientIpByte").getItemsSketch().update(Dimension.setClientIpDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());


                    CollectionValue clientIpByte = stringItemsSketchHashMap.get("clientIpByte").getStringCollectionValueHashMap().get(Dimension.setClientIpDimension(entity));//从key获取集合

                    if (clientIpByte==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                        collectionValue.setSource(entity.getCommon_client_ip());

                        stringItemsSketchHashMap.get("clientIpByte").getStringCollectionValueHashMap().put(Dimension.setClientIpDimension(entity),collectionValue);

                    }else {//做加和

                        clientIpByte.getCollectionValue(clientIpByte,entity);

                        stringItemsSketchHashMap.get("clientIpByte").getStringCollectionValueHashMap().put(Dimension.setClientIpDimension(entity),clientIpByte);


                    }



                }
                break;
            case "serverIpByte":
                if(stringItemsSketchHashMap.isEmpty()) {

                    ItemsSketch<String> serverIpByteItemsSketch = new ItemsSketch<>(1048576);

                    serverIpByteItemsSketch.update(Dimension.setServerIpDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());

                    CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                    collectionValue.setSource(entity.getCommon_server_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setServerIpDimension(entity),collectionValue);





                    DimensionItemsSketch serverIpByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.SERVERIP,serverIpByteItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("serverIpByte",serverIpByteDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("serverIpByte").getItemsSketch().update(Dimension.setServerIpDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());

                    CollectionValue serverIpByte = stringItemsSketchHashMap.get("serverIpByte").getStringCollectionValueHashMap().get(Dimension.setServerIpDimension(entity));


                    if (serverIpByte==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                        collectionValue.setSource(entity.getCommon_server_ip());

                        stringItemsSketchHashMap.get("serverIpByte").getStringCollectionValueHashMap().put(Dimension.setServerIpDimension(entity),collectionValue);

                    }else {//做加和

                        serverIpByte.getCollectionValue(serverIpByte,entity);

                        stringItemsSketchHashMap.get("serverIpByte").getStringCollectionValueHashMap().put(Dimension.setServerIpDimension(entity),serverIpByte);


                    }



                }
                break;
            case "internalIpByte":
                if(stringItemsSketchHashMap.isEmpty()) {


                    ItemsSketch<String> internalIpByteItemsSketch = new ItemsSketch<>(1048576);

                    internalIpByteItemsSketch.update(Dimension.setInternalIpDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());

                    CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                    collectionValue.setSource(entity.getCommon_internal_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setInternalIpDimension(entity),collectionValue);






                    DimensionItemsSketch internalIpByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.INTERNALIP,internalIpByteItemsSketch,stringCollectionValueHashMap);


                    stringItemsSketchHashMap.put("internalIpByte",internalIpByteDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("internalIpByte").getItemsSketch().update(Dimension.setInternalIpDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());


                    CollectionValue internalIpByte = stringItemsSketchHashMap.get("internalIpByte").getStringCollectionValueHashMap().get(Dimension.setInternalIpDimension(entity));


                    if (internalIpByte==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                        collectionValue.setSource(entity.getCommon_server_ip());

                        stringItemsSketchHashMap.get("internalIpByte").getStringCollectionValueHashMap().put(Dimension.setInternalIpDimension(entity),collectionValue);

                    }else {//做加和

                        internalIpByte.getCollectionValue(internalIpByte,entity);

                        stringItemsSketchHashMap.get("internalIpByte").getStringCollectionValueHashMap().put(Dimension.setInternalIpDimension(entity),internalIpByte);


                    }





                }
                break;
            case "externalIpByte":
                if(stringItemsSketchHashMap.isEmpty()) {



                    ItemsSketch<String> externalIpByteItemsSketch = new ItemsSketch<>(1048576);

                    externalIpByteItemsSketch.update(Dimension.setExternalIpDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());


                    CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                    collectionValue.setSource(entity.getCommon_external_ip());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setExternalIpDimension(entity),collectionValue);





                    DimensionItemsSketch externalIpByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.EXTERNALIP,externalIpByteItemsSketch,stringCollectionValueHashMap);



                    stringItemsSketchHashMap.put("externalIpByte",externalIpByteDimensionItemsSketch);

                }else {

                    stringItemsSketchHashMap.get("externalIpByte").getItemsSketch().update(Dimension.setExternalIpDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());

                    CollectionValue externalIpByte = stringItemsSketchHashMap.get("externalIpByte").getStringCollectionValueHashMap().get(Dimension.setExternalIpDimension(entity));


                    if (externalIpByte==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                        collectionValue.setSource(entity.getCommon_external_ip());

                        stringItemsSketchHashMap.get("externalIpByte").getStringCollectionValueHashMap().put(Dimension.setExternalIpDimension(entity),collectionValue);

                    }else {//做加和

                        externalIpByte.getCollectionValue(externalIpByte,entity);

                        stringItemsSketchHashMap.get("externalIpByte").getStringCollectionValueHashMap().put(Dimension.setExternalIpDimension(entity),externalIpByte);


                    }






                }

                break;
            case "domainByte":
                if(stringItemsSketchHashMap.isEmpty()) {


                    ItemsSketch<String> domainByteItemsSketch = new ItemsSketch<>(1048576);

                    domainByteItemsSketch.update(Dimension.setDomainDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());

                    CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                    collectionValue.setSource(entity.getHttp_domain());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setDomainDimension(entity),collectionValue);






                    DimensionItemsSketch domainByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.DOMAIN,domainByteItemsSketch,stringCollectionValueHashMap);

                    stringItemsSketchHashMap.put("domainByte",domainByteDimensionItemsSketch);

                }else {
                    stringItemsSketchHashMap.get("domainByte").getItemsSketch().update(Dimension.setDomainDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());

                    CollectionValue domainByte = stringItemsSketchHashMap.get("domainByte").getStringCollectionValueHashMap().get(Dimension.setDomainDimension(entity));


                    if (domainByte==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                        collectionValue.setSource(entity.getHttp_domain());

                        stringItemsSketchHashMap.get("domainByte").getStringCollectionValueHashMap().put(Dimension.setDomainDimension(entity),collectionValue);

                    }else {//做加和

                        domainByte.getCollectionValue(domainByte,entity);

                        stringItemsSketchHashMap.get("domainByte").getStringCollectionValueHashMap().put(Dimension.setDomainDimension(entity),domainByte);


                    }





                }

                break;
            case "subscriberIdByte":
                if(stringItemsSketchHashMap.isEmpty()) {


                    ItemsSketch<String> subscriberIdByteItemsSketch = new ItemsSketch<>(1048576);

                    subscriberIdByteItemsSketch.update(Dimension.setSubscriberIdDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());


                    CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                    collectionValue.setSource(entity.getCommon_subscriber_id());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setSubscriberIdDimension(entity),collectionValue);





                    DimensionItemsSketch subscriberIdByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.SUBSCRIBERID,subscriberIdByteItemsSketch,stringCollectionValueHashMap);


                    stringItemsSketchHashMap.put("subscriberIdByte",subscriberIdByteDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("subscriberIdByte").getItemsSketch().update(Dimension.setSubscriberIdDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());

                    CollectionValue subscriberIdByte = stringItemsSketchHashMap.get("subscriberIdByte").getStringCollectionValueHashMap().get(Dimension.setSubscriberIdDimension(entity));


                    if (subscriberIdByte==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                        collectionValue.setSource(entity.getCommon_subscriber_id());

                        stringItemsSketchHashMap.get("subscriberIdByte").getStringCollectionValueHashMap().put(Dimension.setSubscriberIdDimension(entity),collectionValue);

                    }else {//做加和

                        subscriberIdByte.getCollectionValue(subscriberIdByte,entity);

                        stringItemsSketchHashMap.get("subscriberIdByte").getStringCollectionValueHashMap().put(Dimension.setSubscriberIdDimension(entity),subscriberIdByte);


                    }






                }

                break;
            case "appLabelByte":
                if(stringItemsSketchHashMap.isEmpty()) {


                    ItemsSketch<String> appLabelByteItemsSketch = new ItemsSketch<>(1048576);


                    appLabelByteItemsSketch.update(Dimension.setAppLabelDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());


                    CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                    collectionValue.setSource(entity.getCommon_app_label());
                    HashMap<String, CollectionValue> stringCollectionValueHashMap = new HashMap<>();
                    stringCollectionValueHashMap.put(Dimension.setAppLabelDimension(entity),collectionValue);





                    DimensionItemsSketch appLabelByteDimensionItemsSketch = new DimensionItemsSketch(Dimension.APPLABEL,appLabelByteItemsSketch,stringCollectionValueHashMap);


                    stringItemsSketchHashMap.put("appLabelByte",appLabelByteDimensionItemsSketch);


                }else {

                    stringItemsSketchHashMap.get("appLabelByte").getItemsSketch().update(Dimension.setAppLabelDimension(entity),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());

                    CollectionValue appLabelByte = stringItemsSketchHashMap.get("appLabelByte").getStringCollectionValueHashMap().get(Dimension.setAppLabelDimension(entity));


                    if (appLabelByte==null){

                        CollectionValue collectionValue = new CollectionValue(entity,"bytes");
                        collectionValue.setSource(entity.getCommon_app_label());

                        stringItemsSketchHashMap.get("appLabelByte").getStringCollectionValueHashMap().put(Dimension.setAppLabelDimension(entity),collectionValue);

                    }else {//做加和

                        appLabelByte.getCollectionValue(appLabelByte,entity);

                        stringItemsSketchHashMap.get("appLabelByte").getStringCollectionValueHashMap().put(Dimension.setAppLabelDimension(entity),appLabelByte);


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
