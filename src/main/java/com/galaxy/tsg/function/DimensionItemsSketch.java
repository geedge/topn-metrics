package com.galaxy.tsg.function;


import org.apache.datasketches.frequencies.ItemsSketch;

/**
 * @author fy
 * @version 1.0
 * @date 2023/2/17 10:03
 * 维度+datasketches
 */
public class DimensionItemsSketch {

    private String dimension;//维度
    private ItemsSketch<String> itemsSketch;//对应的


    public DimensionItemsSketch(String dimension, ItemsSketch<String> itemsSketch) {
        this.dimension = dimension;
        this.itemsSketch = itemsSketch;
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
}




