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
public class DatasketchForMetricsAggregate2 implements AggregateFunction<Entity, HashMap<String,DimensionItemsSketch>, HashMap<String,DimensionItemsSketch>> {

    private String key ;


    public DatasketchForMetricsAggregate2(String key ){

        this.key = key;
    }


    @Override
    public HashMap<String, DimensionItemsSketch> createAccumulator() {
        return new HashMap<String, DimensionItemsSketch>(32768);
    }

    @Override
    public HashMap<String, DimensionItemsSketch> add(Entity entity, HashMap<String, DimensionItemsSketch> stringItemsSketchHashMap) {

//        String dimension = cnRecordLog.getCommon_client_ip();//维度
//        System.out.println(dimension);


        if(stringItemsSketchHashMap.isEmpty()) {

            ItemsSketch<String> sessionItemsSketch = new ItemsSketch<>(32768);//新建
            ItemsSketch<String> pktItemsSketch = new ItemsSketch<>(32768);//新建
            ItemsSketch<String> byteItemsSketch = new ItemsSketch<>(32768);//新建

            sessionItemsSketch.update(Dimension.setDimensionValue(entity,key),entity.getCommon_sessions());
            pktItemsSketch.update(Dimension.setDimensionValue(entity,key),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());
            byteItemsSketch.update(Dimension.setDimensionValue(entity,key),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());


            CollectionValue sessionsCollectionValue = new CollectionValue(entity,"sessions",key);
//                    sessionsCollectionValue.setSource(entity.getCommon_client_ip());
            HashMap<String, CollectionValue> sessionsStringCollectionValueHashMap = new HashMap<>();
            sessionsStringCollectionValueHashMap.put(Dimension.setDimensionValue(entity,key),sessionsCollectionValue);

            CollectionValue packetsCollectionValue = new CollectionValue(entity,"packets",key);
//                    packetsCollectionValue.setSource(entity.getCommon_client_ip());
            HashMap<String, CollectionValue> packetsStringCollectionValueHashMap = new HashMap<>();
            packetsStringCollectionValueHashMap.put(Dimension.setDimensionValue(entity,key),packetsCollectionValue);

            CollectionValue bytesCollectionValue = new CollectionValue(entity,"bytes",key);
//                    bytesCollectionValue.setSource(entity.getCommon_client_ip());
            HashMap<String, CollectionValue> bytesStringCollectionValueHashMap = new HashMap<>();
            bytesStringCollectionValueHashMap.put(Dimension.setDimensionValue(entity,key),bytesCollectionValue);



            DimensionItemsSketch sessionDimensionItemsSketch = new DimensionItemsSketch(Dimension.setDimensionTag(key), sessionItemsSketch,sessionsStringCollectionValueHashMap);
            DimensionItemsSketch packetsDimensionItemsSketch = new DimensionItemsSketch(Dimension.setDimensionTag(key), pktItemsSketch,packetsStringCollectionValueHashMap);
            DimensionItemsSketch bytesDimensionItemsSketch = new DimensionItemsSketch(Dimension.setDimensionTag(key), byteItemsSketch,bytesStringCollectionValueHashMap);


            stringItemsSketchHashMap.put(key+"Session", sessionDimensionItemsSketch);
            stringItemsSketchHashMap.put(key+"Packets", packetsDimensionItemsSketch);
            stringItemsSketchHashMap.put(key+"Bytes", bytesDimensionItemsSketch);




        }else {

            stringItemsSketchHashMap.get(key+"Session").getItemsSketch().update(Dimension.setDimensionValue(entity,key),entity.getCommon_sessions());
            stringItemsSketchHashMap.get(key+"Packets").getItemsSketch().update(Dimension.setDimensionValue(entity,key),entity.getCommon_c2s_pkt_num()+entity.getCommon_s2c_pkt_num());
            stringItemsSketchHashMap.get(key+"Bytes").getItemsSketch().update(Dimension.setDimensionValue(entity,key),entity.getCommon_c2s_byte_num()+entity.getCommon_s2c_byte_num());



            CollectionValue sessionsCollectionValue = stringItemsSketchHashMap.get(key+"Session").getStringCollectionValueHashMap().get(Dimension.setDimensionValue(entity,key));//从key获取集合
            CollectionValue packetsCollectionValue = stringItemsSketchHashMap.get(key+"Packets").getStringCollectionValueHashMap().get(Dimension.setDimensionValue(entity,key));//从key获取集合
            CollectionValue bytesCollectionValue = stringItemsSketchHashMap.get(key+"Bytes").getStringCollectionValueHashMap().get(Dimension.setDimensionValue(entity,key));//从key获取集合



            if (sessionsCollectionValue==null){

                sessionsCollectionValue = new CollectionValue(entity,"sessions",key);
                sessionsCollectionValue.setSource(entity.getCommon_client_ip());

                packetsCollectionValue = new CollectionValue(entity,"packets",key);
                packetsCollectionValue.setSource(entity.getCommon_client_ip());

                bytesCollectionValue = new CollectionValue(entity,"bytes",key);
                bytesCollectionValue.setSource(entity.getCommon_client_ip());


                stringItemsSketchHashMap.get(key+"Session").getStringCollectionValueHashMap().put(Dimension.setDimensionValue(entity,key),sessionsCollectionValue);
                stringItemsSketchHashMap.get(key+"Packets").getStringCollectionValueHashMap().put(Dimension.setDimensionValue(entity,key),packetsCollectionValue);
                stringItemsSketchHashMap.get(key+"Bytes").getStringCollectionValueHashMap().put(Dimension.setDimensionValue(entity,key),bytesCollectionValue);



            }else {//做加和

                sessionsCollectionValue.getCollectionValue(sessionsCollectionValue,entity);
                packetsCollectionValue.getCollectionValue(packetsCollectionValue,entity);
                bytesCollectionValue.getCollectionValue(bytesCollectionValue,entity);



                stringItemsSketchHashMap.get(key+"Session").getStringCollectionValueHashMap().put(Dimension.setDimensionValue(entity,key),sessionsCollectionValue);
                stringItemsSketchHashMap.get(key+"Packets").getStringCollectionValueHashMap().put(Dimension.setDimensionValue(entity,key),packetsCollectionValue);
                stringItemsSketchHashMap.get(key+"Bytes").getStringCollectionValueHashMap().put(Dimension.setDimensionValue(entity,key),bytesCollectionValue);

            }


        }



        return stringItemsSketchHashMap;
    }

    @Override
    public HashMap<String, DimensionItemsSketch> getResult(HashMap<String, DimensionItemsSketch> stringItemsSketchHashMap) {


        return stringItemsSketchHashMap;
    }

    @Override
    public HashMap<String, DimensionItemsSketch> merge(HashMap<String, DimensionItemsSketch> stringDimensionItemsSketchHashMap, HashMap<String, DimensionItemsSketch> acc1) {


        return null;
    }
}
