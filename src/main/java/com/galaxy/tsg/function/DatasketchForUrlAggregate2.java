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
public class DatasketchForUrlAggregate2 implements AggregateFunction<UrlEntity, HashMap<String,DimensionItemsSketch>, HashMap<String,DimensionItemsSketch>> {


    @Override
    public HashMap<String, DimensionItemsSketch> createAccumulator() {
        return new HashMap<String, DimensionItemsSketch>(1048576);
    }

    @Override
    public HashMap<String, DimensionItemsSketch> add(UrlEntity urlEntity, HashMap<String, DimensionItemsSketch> stringItemsSketchHashMap) {

//        String dimension = cnRecordLog.getCommon_client_ip();//维度
//        System.out.println(dimension);

        if(stringItemsSketchHashMap.isEmpty()) {

            ItemsSketch<String> urlSessionItemsSketch = new ItemsSketch<>(1048576);//新建

            urlSessionItemsSketch.update(Dimension.setUrlDimension(urlEntity),urlEntity.getCommon_sessions());


            TopUrlEntity topUrlEntity = new TopUrlEntity();
            topUrlEntity.setSession_num(urlEntity.getCommon_sessions());
            topUrlEntity.setStat_time(System.currentTimeMillis() / 1000);
            topUrlEntity.setUrl(urlEntity.getHttp_url());
            topUrlEntity.setVsys_id(urlEntity.getCommon_vsys_id());
            HashMap<String,TopUrlEntity> stringTopUrlEntityHashMap = new HashMap<>();
            stringTopUrlEntityHashMap.put(Dimension.setUrlDimension(urlEntity),topUrlEntity);


            DimensionItemsSketch urlSessionDimensionItemsSketch = new DimensionItemsSketch(stringTopUrlEntityHashMap,Dimension.URL,urlSessionItemsSketch);

            stringItemsSketchHashMap.put("urlSession", urlSessionDimensionItemsSketch);


        }else {


            stringItemsSketchHashMap.get("urlSession").getItemsSketch().update(Dimension.setUrlDimension(urlEntity),urlEntity.getCommon_sessions());

            TopUrlEntity urlSession = stringItemsSketchHashMap.get("urlSession").getStringTopUrlEntityHashMap().get(Dimension.setUrlDimension(urlEntity));//从key获取集合

            if (urlSession==null){

                TopUrlEntity topUrlEntity = new TopUrlEntity();
                topUrlEntity.setSession_num(urlEntity.getCommon_sessions());
                topUrlEntity.setStat_time(System.currentTimeMillis() / 1000);
                topUrlEntity.setUrl(urlEntity.getHttp_url());
                topUrlEntity.setVsys_id(urlEntity.getCommon_vsys_id());

                stringItemsSketchHashMap.get("urlSession").getStringTopUrlEntityHashMap().put(Dimension.setUrlDimension(urlEntity),urlSession);

            }else {//做加和


                urlSession.setSession_num(urlSession.getSession_num()+urlEntity.getCommon_sessions());


                stringItemsSketchHashMap.get("urlSession").getStringTopUrlEntityHashMap().put(Dimension.setUrlDimension(urlEntity),urlSession);


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
