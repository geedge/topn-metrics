package com.galaxy.tsg.function;

import com.alibaba.fastjson.JSONObject;
import com.galaxy.tsg.pojo.TopUrlEntity;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.galaxy.tsg.config.commonConfig.TOP_LIMIT;

/**
 * @author fy
 * @version 1.0
 * @date 2023/3/7 15:50
 */
public class UserCountWindowResult6 extends ProcessWindowFunction<HashMap<String, DimensionItemsSketch>, String, Tuple4<String, Long, String, String>,TimeWindow> {


    @Override
    public void process(Tuple4<String, Long, String, String> stringLongStringStringTuple4, Context context, Iterable<HashMap<String, DimensionItemsSketch>> iterable, Collector<String> collector) throws Exception {


        HashMap<String, DimensionItemsSketch> dataHashMap = iterable.iterator().next();


//        System.out.println(dataHashMap.toString());

        Set<Map.Entry<String, DimensionItemsSketch>> entries = dataHashMap.entrySet();


        for (Map.Entry<String, DimensionItemsSketch> entry : entries) {

//            System.out.println(entry.getKey()+"");
//            stringBuilder.append(entry.getKey()+"\n");

            ItemsSketch.Row<String>[] items = entry.getValue().getItemsSketch().getFrequentItems(ErrorType.NO_FALSE_POSITIVES);

            for (int i = 0; i < items.length; i++) {


//                String resultStr = "No." + (i + 1) + " "
//                        + "ip:" + items[i].getItem() + " "
//                        + " Est:" + items[i].getEstimate()
//                        + " UB:" + items[i].getUpperBound()
//                        + " LB:" + items[i].getLowerBound();

                String jsonStr = "";
                if (!entry.getKey().equals("urlSession")) {
                    CollectionValue collectionValue = entry.getValue().getStringCollectionValueHashMap().get(items[i].getItem());
                    collectionValue.setStat_time(context.window().getEnd() / 1000);
                    jsonStr = JSONObject.toJSONString(collectionValue);
                } else {
                    TopUrlEntity topUrlEntity = entry.getValue().getStringTopUrlEntityHashMap().get(items[i].getItem());
                    topUrlEntity.setStat_time(context.window().getEnd() / 1000);
                    jsonStr = JSONObject.toJSONString(topUrlEntity);
                }


                collector.collect(jsonStr);

//                String item = items[i].toString();
//                stringBuilder.append(resultStr);

//                stringBuilder.append("\n");


                if (i == TOP_LIMIT)//够条数就结束
                    break;

            }


        }


    }


}
