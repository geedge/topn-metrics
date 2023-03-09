package com.galaxy.tsg.function;


import com.galaxy.tsg.pojo.TopUrlEntity;
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
public class DatasketchForUrlAggregate implements AggregateFunction<UrlEntity, HashMap<String,DimensionItemsSketch>, HashMap<String,DimensionItemsSketch>> {


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


            TopUrlEntity topUrlEntity = new TopUrlEntity();
            topUrlEntity.setSession_num(cnRecordLog.getCommon_sessions());
            topUrlEntity.setStat_time(System.currentTimeMillis() / 1000);
            topUrlEntity.setUrl(cnRecordLog.getHttp_url());
            topUrlEntity.setVsys_id(cnRecordLog.getCommon_vsys_id());
            HashMap<String,TopUrlEntity> stringTopUrlEntityHashMap = new HashMap<>();
            stringTopUrlEntityHashMap.put(Dimension.setEightDimension(cnRecordLog),topUrlEntity);


            DimensionItemsSketch eightSessionDimensionItemsSketch = new DimensionItemsSketch(stringTopUrlEntityHashMap,Dimension.EIGHT,eightSessionItemsSketch);

            stringItemsSketchHashMap.put("eightSession", eightSessionDimensionItemsSketch);


        }else {


            stringItemsSketchHashMap.get("eightSession").getItemsSketch().update(Dimension.setEightDimension(cnRecordLog),cnRecordLog.getCommon_sessions());

            TopUrlEntity eightSession = stringItemsSketchHashMap.get("eightSession").getStringTopUrlEntityHashMap().get(Dimension.setEightDimension(cnRecordLog));//从key获取集合

            if (eightSession==null){

                TopUrlEntity topUrlEntity = new TopUrlEntity();
                topUrlEntity.setSession_num(cnRecordLog.getCommon_sessions());
                topUrlEntity.setStat_time(System.currentTimeMillis() / 1000);
                topUrlEntity.setUrl(cnRecordLog.getHttp_url());
                topUrlEntity.setVsys_id(cnRecordLog.getCommon_vsys_id());

                stringItemsSketchHashMap.get("eightSession").getStringTopUrlEntityHashMap().put(Dimension.setEightDimension(cnRecordLog),eightSession);

            }else {//做加和


                eightSession.setSession_num(eightSession.getSession_num()+cnRecordLog.getCommon_sessions());


                stringItemsSketchHashMap.get("eightSession").getStringTopUrlEntityHashMap().put(Dimension.setEightDimension(cnRecordLog),eightSession);


            }






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
