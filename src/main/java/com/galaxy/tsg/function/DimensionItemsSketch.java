package com.galaxy.tsg.function;


import com.galaxy.tsg.pojo.TopUrlEntity;
import org.apache.datasketches.frequencies.ItemsSketch;

import java.util.HashMap;

/**
 * @author fy
 * @version 1.0
 * @date 2023/2/17 10:03
 * 维度+datasketches
 */
public class DimensionItemsSketch {

    private String dimension;//维度
    private ItemsSketch<String> itemsSketch;//对应的

    private HashMap<String,CollectionValue> stringCollectionValueHashMap;//同指标集合成的值

    private HashMap<String, TopUrlEntity> stringTopUrlEntityHashMap;//同指标集合成的值



    public DimensionItemsSketch(String dimension, ItemsSketch<String> itemsSketch) {
        this.dimension = dimension;
        this.itemsSketch = itemsSketch;
    }


    public DimensionItemsSketch(String dimension, ItemsSketch<String> itemsSketch, HashMap<String, CollectionValue> stringCollectionValueHashMap) {
        this.dimension = dimension;
        this.itemsSketch = itemsSketch;
        this.stringCollectionValueHashMap = stringCollectionValueHashMap;
    }


    public DimensionItemsSketch(HashMap<String, TopUrlEntity> stringTopUrlEntityHashMap, String dimension, ItemsSketch<String> itemsSketch) {
        this.dimension = dimension;
        this.itemsSketch = itemsSketch;
        this.stringTopUrlEntityHashMap = stringTopUrlEntityHashMap;
    }

    public String getDimension() {
        return dimension;
    }

    public void setDimension(String dimension) {
        this.dimension = dimension;
    }

    public ItemsSketch<String> getItemsSketch() {
        return itemsSketch;
    }

    public void setItemsSketch(ItemsSketch<String> itemsSketch) {
        this.itemsSketch = itemsSketch;
    }


    public HashMap<String, CollectionValue> getStringCollectionValueHashMap() {
        return stringCollectionValueHashMap;
    }

    public void setStringCollectionValueHashMap(HashMap<String, CollectionValue> stringCollectionValueHashMap) {
        this.stringCollectionValueHashMap = stringCollectionValueHashMap;
    }


    public HashMap<String, TopUrlEntity> getStringTopUrlEntityHashMap() {
        return stringTopUrlEntityHashMap;
    }

    public void setStringTopUrlEntityHashMap(HashMap<String, TopUrlEntity> stringTopUrlEntityHashMap) {
        this.stringTopUrlEntityHashMap = stringTopUrlEntityHashMap;
    }
}




