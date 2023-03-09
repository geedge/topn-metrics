package com.galaxy.tsg.function;

import com.alibaba.fastjson.JSONObject;
import com.galaxy.tsg.pojo.Entity;
import com.galaxy.tsg.pojo.UrlEntity;

/**
 * @author fy
 * @version 1.0
 * @date 2023/3/8 19:46
 * 同指标集合成的值
 */
public class CollectionValue {

    public long c2s_byte_num;
    public long c2s_pkt_num;
    public long s2c_byte_num ;
    public long s2c_pkt_num ;
    public long session_num ;
    public long stat_time ;
    public long vsys_id ;
    public String data_center;
    public String device_group;
    public String order_by;
    public String source;

    public CollectionValue(long c2s_byte_num, long c2s_pkt_num, long s2c_byte_num, long s2c_pkt_num, long session_num, long stat_time, long vsys_id, String data_center, String device_group, String order_by, String source) {
        this.c2s_byte_num = c2s_byte_num;
        this.c2s_pkt_num = c2s_pkt_num;
        this.s2c_byte_num = s2c_byte_num;
        this.s2c_pkt_num = s2c_pkt_num;
        this.session_num = session_num;
        this.stat_time = stat_time;
        this.vsys_id = vsys_id;
        this.data_center = data_center;
        this.device_group = device_group;
        this.order_by = order_by;
        this.source = source;
    }


    public CollectionValue(Entity entity,String key ) {



        this.c2s_byte_num = entity.getCommon_c2s_byte_num();
        this.c2s_pkt_num = entity.getCommon_c2s_pkt_num();
        this.s2c_byte_num = entity.getCommon_s2c_byte_num();
        this.s2c_pkt_num = entity.getCommon_s2c_pkt_num();
        this.session_num = entity.getCommon_sessions();
        this.stat_time = System.currentTimeMillis() / 1000;
        this.vsys_id = entity.getCommon_vsys_id();
        this.data_center = entity.getCommon_data_center();
        this.device_group = entity.getCommon_device_group();
        this.order_by = key;
//        this.source = source;//分情况

    }



    public CollectionValue getCollectionValue(CollectionValue collectionValue,Entity entity){

        collectionValue.setC2s_byte_num(collectionValue.getC2s_byte_num()+entity.getCommon_c2s_byte_num());

        collectionValue.setC2s_pkt_num(collectionValue.getC2s_pkt_num()+entity.getCommon_c2s_pkt_num());

        collectionValue.setS2c_byte_num(collectionValue.getS2c_byte_num()+entity.getCommon_s2c_byte_num());

        collectionValue.setS2c_pkt_num(collectionValue.getS2c_pkt_num()+entity.getCommon_s2c_pkt_num());

        collectionValue.setSession_num(collectionValue.getSession_num()+entity.getCommon_sessions());

        return collectionValue;

    }








    public long getC2s_byte_num() {
        return c2s_byte_num;
    }

    public void setC2s_byte_num(long c2s_byte_num) {
        this.c2s_byte_num = c2s_byte_num;
    }

    public long getC2s_pkt_num() {
        return c2s_pkt_num;
    }

    public void setC2s_pkt_num(long c2s_pkt_num) {
        this.c2s_pkt_num = c2s_pkt_num;
    }

    public long getS2c_byte_num() {
        return s2c_byte_num;
    }

    public void setS2c_byte_num(long s2c_byte_num) {
        this.s2c_byte_num = s2c_byte_num;
    }

    public long getS2c_pkt_num() {
        return s2c_pkt_num;
    }

    public void setS2c_pkt_num(long s2c_pkt_num) {
        this.s2c_pkt_num = s2c_pkt_num;
    }

    public long getSession_num() {
        return session_num;
    }

    public void setSession_num(long session_num) {
        this.session_num = session_num;
    }

    public long getStat_time() {
        return stat_time;
    }

    public void setStat_time(long stat_time) {
        this.stat_time = stat_time;
    }

    public long getVsys_id() {
        return vsys_id;
    }

    public void setVsys_id(long vsys_id) {
        this.vsys_id = vsys_id;
    }

    public String getData_center() {
        return data_center;
    }

    public void setData_center(String data_center) {
        this.data_center = data_center;
    }

    public String getDevice_group() {
        return device_group;
    }

    public void setDevice_group(String device_group) {
        this.device_group = device_group;
    }

    public String getOrder_by() {
        return order_by;
    }

    public void setOrder_by(String order_by) {
        this.order_by = order_by;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;

    }


    @Override
    public String toString() {
        return "CollectionValue{" +
                "c2s_byte_num=" + c2s_byte_num +
                ", c2s_pkt_num=" + c2s_pkt_num +
                ", s2c_byte_num=" + s2c_byte_num +
                ", s2c_pkt_num=" + s2c_pkt_num +
                ", session_num=" + session_num +
                ", stat_time=" + stat_time +
                ", vsys_id=" + vsys_id +
                ", data_center='" + data_center + '\'' +
                ", device_group='" + device_group + '\'' +
                ", order_by='" + order_by + '\'' +
                ", source='" + source + '\'' +
                '}';
    }
}
