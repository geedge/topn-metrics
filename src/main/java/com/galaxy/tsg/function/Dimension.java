package com.galaxy.tsg.function;


import com.galaxy.tsg.pojo.Entity;
import com.galaxy.tsg.pojo.UrlEntity;

/**
 * @author fy
 * @version 1.0
 * @date 2023/2/17 10:14
 * 各种维度
 */
public class Dimension {

    public static final String ONE = "common_client_ip,common_vsys_id,common_device_group,common_data_center";
    public static final String TWO = "common_server_ip,common_vsys_id,common_device_group,common_data_center";
    public static final String THREE = "common_internal_ip,common_vsys_id,common_device_group,common_data_center";
    public static final String FOUR = "common_external_ip,common_vsys_id,common_device_group,common_data_center";
    public static final String FIVE = "http_domain,common_vsys_id,common_device_group,common_data_center";
    public static final String SIX = "common_subscriber_id,common_vsys_id,common_device_group,common_data_center";
    public static final String SEVEN = "common_app_label,common_vsys_id,common_device_group,common_data_center";

    public static final String EIGHT = "http_url,common_vsys_id";



    public static String setOneDimension(Entity cnRecordLog){

        String oneDimension = cnRecordLog.getCommon_client_ip()+","+cnRecordLog.getCommon_vsys_id()+","+cnRecordLog.getCommon_device_group()+","+cnRecordLog.getCommon_data_center();
        return oneDimension;
    }


    public static String setTwoDimension(Entity cnRecordLog){

        String twoDimension = cnRecordLog.getCommon_server_ip()+","+cnRecordLog.getCommon_vsys_id()+","+cnRecordLog.getCommon_device_group()+","+cnRecordLog.getCommon_data_center();
        return twoDimension;
    }


    public static String setThreeDimension(Entity cnRecordLog){

        String threeDimension = cnRecordLog.getCommon_internal_ip()+","+cnRecordLog.getCommon_vsys_id()+","+cnRecordLog.getCommon_device_group()+","+cnRecordLog.getCommon_data_center();
        return threeDimension;
    }


    public static String setFourDimension(Entity cnRecordLog){

        String fourDimension = cnRecordLog.getCommon_external_ip()+","+cnRecordLog.getCommon_vsys_id()+","+cnRecordLog.getCommon_device_group()+","+cnRecordLog.getCommon_data_center();
        return fourDimension;
    }

    public static String setFiveDimension(Entity cnRecordLog){

        String fiveDimension = cnRecordLog.getHttp_domain()+","+cnRecordLog.getCommon_vsys_id()+","+cnRecordLog.getCommon_device_group()+","+cnRecordLog.getCommon_data_center();
        return fiveDimension;
    }


    public static String setSixDimension(Entity cnRecordLog){

        String sixDimension = cnRecordLog.getCommon_subscriber_id()+","+cnRecordLog.getCommon_vsys_id()+","+cnRecordLog.getCommon_device_group()+","+cnRecordLog.getCommon_data_center();
        return sixDimension;
    }


    public static String setSevenDimension(Entity cnRecordLog){

        String sevenDimension = cnRecordLog.getCommon_app_label()+","+cnRecordLog.getCommon_vsys_id()+","+cnRecordLog.getCommon_device_group()+","+cnRecordLog.getCommon_data_center();
        return sevenDimension;
    }

    public static String setEightDimension(UrlEntity cnRecordLog){

        String eightDimension = cnRecordLog.getHttp_url()+","+cnRecordLog.getCommon_vsys_id();
        return eightDimension;
    }

}
