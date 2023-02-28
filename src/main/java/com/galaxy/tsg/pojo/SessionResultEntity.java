package com.galaxy.tsg.pojo;

public class SessionResultEntity implements Comparable<SessionResultEntity> {


    private String source ;
    private Long vsys_id ;
    private Long session_num ;
    private Long c2s_pkt_num ;
    private Long s2c_pkt_num;
    private Long c2s_byte_num ;
    private Long s2c_byte_num ;
    private String order_by;
    private String device_group;
    private String data_center;
    private Long stat_time;
    private String destination ;
    private String domain;
    private String subscriber_id;
    private String app_name;

    public String getApp_name() {
        return app_name;
    }

    public void setApp_name(String app_name) {
        this.app_name = app_name;
    }

    public String getSubscriber_id() {
        return subscriber_id;
    }

    public void setSubscriber_id(String subscriber_id) {
        this.subscriber_id = subscriber_id;
    }
    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }
    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Long getVsys_id() {
        return vsys_id;
    }

    public void setVsys_id(Long vsys_id) {
        this.vsys_id = vsys_id;
    }

    public Long getSession_num() {
        return session_num;
    }

    public void setSession_num(Long session_num) {
        this.session_num = session_num;
    }

    public Long getC2s_pkt_num() {
        return c2s_pkt_num;
    }

    public void setC2s_pkt_num(Long c2s_pkt_num) {
        this.c2s_pkt_num = c2s_pkt_num;
    }

    public Long getS2c_pkt_num() {
        return s2c_pkt_num;
    }

    public void setS2c_pkt_num(Long s2c_pkt_num) {
        this.s2c_pkt_num = s2c_pkt_num;
    }

    public Long getC2s_byte_num() {
        return c2s_byte_num;
    }

    public void setC2s_byte_num(Long c2s_byte_num) {
        this.c2s_byte_num = c2s_byte_num;
    }

    public Long getS2c_byte_num() {
        return s2c_byte_num;
    }

    public void setS2c_byte_num(Long s2c_byte_num) {
        this.s2c_byte_num = s2c_byte_num;
    }

    public String getOrder_by() {
        return order_by;
    }

    public void setOrder_by(String order_by) {
        this.order_by = order_by;
    }

    public String getDevice_group() {
        return device_group;
    }

    public void setDevice_group(String device_group) {
        this.device_group = device_group;
    }

    public String getData_center() {
        return data_center;
    }

    public void setData_center(String data_center) {
        this.data_center = data_center;
    }

    public Long getStat_time() {
        return stat_time;
    }

    public void setStat_time(Long stat_time) {
        this.stat_time = stat_time;
    }

    @Override
    public int compareTo(SessionResultEntity per) {
        if(this.session_num>=per.session_num){
            return 1 ;
        }else{
            return -1 ;
        }
    }



}
