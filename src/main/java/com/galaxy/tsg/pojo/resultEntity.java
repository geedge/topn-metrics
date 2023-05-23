package com.galaxy.tsg.pojo;

public class resultEntity {



    private String order_by;
    private Long stat_time;

    private com.galaxy.tsg.pojo.sessionResultEntity sessionResultEntity;
    private com.galaxy.tsg.pojo.packetResultEntity packetResultEntity;
    private com.galaxy.tsg.pojo.byteResultEntity byteResultEntity;


    public String getOrder_by() {
        return order_by;
    }



    public void setOrder_by(String order_by) {
        this.order_by = order_by;
    }

    public Long getStat_time() {
        return stat_time;
    }

    public void setStat_time(Long stat_time) {
        this.stat_time = stat_time;
    }

    public com.galaxy.tsg.pojo.sessionResultEntity getSessionResultEntity() {
        return sessionResultEntity;
    }

    public void setSessionResultEntity(com.galaxy.tsg.pojo.sessionResultEntity sessionResultEntity) {
        this.sessionResultEntity = sessionResultEntity;
    }

    public com.galaxy.tsg.pojo.packetResultEntity getPacketResultEntity() {
        return packetResultEntity;
    }

    public void setPacketResultEntity(com.galaxy.tsg.pojo.packetResultEntity packetResultEntity) {
        this.packetResultEntity = packetResultEntity;
    }

    public com.galaxy.tsg.pojo.byteResultEntity getByteResultEntity() {
        return byteResultEntity;
    }

    public void setByteResultEntity(com.galaxy.tsg.pojo.byteResultEntity byteResultEntity) {
        this.byteResultEntity = byteResultEntity;
    }
}
