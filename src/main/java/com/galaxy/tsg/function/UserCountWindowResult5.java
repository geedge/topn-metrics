package com.galaxy.tsg.function;

import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author fy
 * @version 1.0
 * @date 2023/3/7 15:50
 */
public class UserCountWindowResult5 extends ProcessAllWindowFunction<HashMap<String, DimensionItemsSketch>, String, TimeWindow> {


    @Override
    public void process(Context context, Iterable<HashMap<String, DimensionItemsSketch>> iterable, Collector<String> collector) throws Exception {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("-------------------------\n");
        stringBuilder.append("datasketches方法窗口结束时间:" + new Timestamp(context.window().getEnd()) + "\n");


        HashMap<String, DimensionItemsSketch> dataHashMap = iterable.iterator().next();


        System.out.println(dataHashMap.toString());

        Set<Map.Entry<String, DimensionItemsSketch>> entries = dataHashMap.entrySet();


        for(Map.Entry<String, DimensionItemsSketch> entry:entries){

            System.out.println(entry.getKey()+"");
            stringBuilder.append(entry.getKey()+"\n");

            ItemsSketch.Row<String>[] items = entry.getValue().getItemsSketch().getFrequentItems(ErrorType.NO_FALSE_POSITIVES);

            for (int i=0;i<items.length;i++){


                String resultStr = "No." + (i + 1) + " "
                        + "ip:" + items[i].getItem() + " "
                        + " Est:" + items[i].getEstimate()
                        + " UB:" + items[i].getUpperBound()
                        + " LB:" + items[i].getLowerBound();



//                String item = items[i].toString();
                stringBuilder.append(resultStr);

                stringBuilder.append("\n");


                if (i==1000)
                    break;

            }





        }

        collector.collect(stringBuilder.toString());

    }




}
