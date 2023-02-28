package com.galaxy.tsg.pojo;

public class ResultEntity  {



    private String order_by;
    private Long stat_time;

    private SessionResultEntity sessionResultEntity;
    private PacketResultEntity packetResultEntity;
    private ByteResultEntity byteResultEntity;
    private TopUrlEntity topUrlEntity;


    public String getOrder_by() {
        return order_by;
    }

    public TopUrlEntity getTopUrlEntity() {
        return topUrlEntity;
    }

    public void setTopUrlEntity(TopUrlEntity topUrlEntity) {
        this.topUrlEntity = topUrlEntity;
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

    public SessionResultEntity getSessionResultEntity() {
        return sessionResultEntity;
    }

    public void setSessionResultEntity(SessionResultEntity sessionResultEntity) {
        this.sessionResultEntity = sessionResultEntity;
    }

    public PacketResultEntity getPacketResultEntity() {
        return packetResultEntity;
    }

    public void setPacketResultEntity(PacketResultEntity packetResultEntity) {
        this.packetResultEntity = packetResultEntity;
    }

    public ByteResultEntity getByteResultEntity() {
        return byteResultEntity;
    }

    public void setByteResultEntity(ByteResultEntity byteResultEntity) {
        this.byteResultEntity = byteResultEntity;
    }
}
