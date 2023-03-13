package com.galaxy.tsg.function;

import com.galaxy.tsg.pojo.ResultEntity;
import com.galaxy.tsg.pojo.SessionResultEntity;
import com.galaxy.tsg.pojo.TopUrlEntity;
import com.galaxy.tsg.pojo.UrlEntity;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author fy
 * @version 1.0
 * @date 2023/3/10 18:27
 */
public class DatasketchUrlCalculate extends ProcessWindowFunction<
        UrlEntity,                  // 输入类型
        ResultEntity,  // 输出类型
        Tuple2<String,Long>,                         // 键类型
        TimeWindow> {

    private final int topSize;



    public DatasketchUrlCalculate(int topSize) {
        this.topSize = topSize;
    }


    @Override
    public void process(Tuple2<String, Long> stringLongTuple2, Context context, Iterable<UrlEntity> iterable, Collector<ResultEntity> collector) throws Exception {


        ItemsSketch<String> sessionItemsSketch = new ItemsSketch<>(1048576);//新建
        HashMap<String, TopUrlEntity> sessionResultEntityHashMap = new HashMap<>();




        for (UrlEntity urlEntity:iterable){


            //处理session
            sessionItemsSketch.update(Dimension.setUrlDimension(urlEntity),urlEntity.getCommon_sessions());

            TopUrlEntity en = new TopUrlEntity();
            en.setSession_num(urlEntity.getCommon_sessions());
            en.setStat_time(context.window().getEnd() / 1000);
            en.setUrl(urlEntity.getHttp_url());
            en.setVsys_id(urlEntity.getCommon_vsys_id());

            sessionResultEntityHashMap.put(Dimension.setUrlDimension(urlEntity),en);

        }


        ItemsSketch.Row<String>[] sessionFrequentItems = sessionItemsSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);

        for (int i=0;i<sessionFrequentItems.length;i++){

            TopUrlEntity topUrlEntity = sessionResultEntityHashMap.get(sessionFrequentItems[i].getItem());

            ResultEntity resultEntity = new ResultEntity();
            resultEntity.setOrder_by("sessions");
            resultEntity.setStat_time(context.window().getEnd() / 1000);
            resultEntity.setTopUrlEntity(topUrlEntity);
            collector.collect(resultEntity);


            if (i==topSize)//够条数就结束
                break;

        }





    }










}
