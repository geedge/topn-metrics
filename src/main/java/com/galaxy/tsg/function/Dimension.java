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

    public static final String CLIENTIP = "common_client_ip,common_vsys_id,common_device_group,common_data_center";
    public static final String SERVERIP = "common_server_ip,common_vsys_id,common_device_group,common_data_center";
    public static final String INTERNALIP = "common_internal_ip,common_vsys_id,common_device_group,common_data_center";
    public static final String EXTERNALIP = "common_external_ip,common_vsys_id,common_device_group,common_data_center";
    public static final String DOMAIN = "http_domain,common_vsys_id,common_device_group,common_data_center";
    public static final String SUBSCRIBERID = "common_subscriber_id,common_vsys_id,common_device_group,common_data_center";
    public static final String APPLABEL = "common_app_label,common_vsys_id,common_device_group,common_data_center";

    public static final String URL = "http_url,common_vsys_id";



    public static String setClientIpDimension(Entity entity){

        String clientIpDimension = entity.getCommon_client_ip()+","+entity.getCommon_vsys_id()+","+entity.getCommon_device_group()+","+entity.getCommon_data_center();
        return clientIpDimension;
    }


    public static String setServerIpDimension(Entity entity){

        String serverIpDimension = entity.getCommon_server_ip()+","+entity.getCommon_vsys_id()+","+entity.getCommon_device_group()+","+entity.getCommon_data_center();
        return serverIpDimension;
    }


    public static String setInternalIpDimension(Entity entity){

        String internalIpDimension = entity.getCommon_internal_ip()+","+entity.getCommon_vsys_id()+","+entity.getCommon_device_group()+","+entity.getCommon_data_center();
        return internalIpDimension;
    }


    public static String setExternalIpDimension(Entity entity){

        String externalIpDimension = entity.getCommon_external_ip()+","+entity.getCommon_vsys_id()+","+entity.getCommon_device_group()+","+entity.getCommon_data_center();
        return externalIpDimension;
    }

    public static String setDomainDimension(Entity entity){

        String domainDimension = entity.getHttp_domain()+","+entity.getCommon_vsys_id()+","+entity.getCommon_device_group()+","+entity.getCommon_data_center();
        return domainDimension;
    }


    public static String setSubscriberIdDimension(Entity entity){

        String subscriberIdDimension = entity.getCommon_subscriber_id()+","+entity.getCommon_vsys_id()+","+entity.getCommon_device_group()+","+entity.getCommon_data_center();
        return subscriberIdDimension;
    }


    public static String setAppLabelDimension(Entity entity){

        String appLabelDimension = entity.getCommon_app_label()+","+entity.getCommon_vsys_id()+","+entity.getCommon_device_group()+","+entity.getCommon_data_center();
        return appLabelDimension;
    }

    public static String setUrlDimension(UrlEntity entity){

        String urlDimension = entity.getHttp_url()+","+entity.getCommon_vsys_id();
        return urlDimension;
    }



    public static String setDimensionValue(Entity entity, String key){

        String dimension = "";

        switch (key) {
            case "common_client_ip":
                dimension = setClientIpDimension(entity);
                break;
            case "common_server_ip":
                dimension = setServerIpDimension(entity);
                break;
            case "common_internal_ip":
                dimension = setInternalIpDimension(entity);
                break;
            case "common_external_ip":
                dimension = setExternalIpDimension(entity);
                break;
            case "http_domain":
                dimension = setDomainDimension(entity);
                break;

            case "common_subscriber_id":
                dimension = setSubscriberIdDimension(entity);
                break;

            case "common_app_label":
                dimension = setAppLabelDimension(entity);
                break;

            default:

        }




        return dimension;
    }



    public static String setDimensionTag(String key){

        String dimension = "";

        switch (key) {
            case "common_client_ip":
                dimension = CLIENTIP;
                break;
            case "common_server_ip":
                dimension = SERVERIP;
                break;
            case "common_internal_ip":
                dimension = INTERNALIP;
                break;
            case "common_external_ip":
                dimension = EXTERNALIP;
                break;
            case "http_domain":
                dimension = DOMAIN;
                break;

            case "common_subscriber_id":
                dimension = SUBSCRIBERID;
                break;

            case "common_app_label":
                dimension = APPLABEL;
                break;

            default:

        }




        return dimension;
    }




}
