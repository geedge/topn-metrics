package com.galaxy.tsg.pojo;

public class packetResultEntity implements Comparable<packetResultEntity>, Cloneable {


    private long timestamp ;


    private String fqdn;
    private String server_ip ;
    private String domain ;
    private long vsys_id ;
    private String device_group ;
    private String client_ip ;
    private String device_id;
    private String data_center;
    private String internal_ip;
    private String external_ip;
    private String subscriber_id;
    private long sessions;
    private long out_bytes;
    private long in_bytes;
    private long out_pkts ;
    private long in_pkts ;
    private String key_by;
    private String l4_protocol;

    private String order_by;


    public String getOrder_by() {
        return order_by;
    }

    public void setOrder_by(String order_by) {
        this.order_by = order_by;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getFqdn() {
        return fqdn;
    }

    public void setFqdn(String fqdn) {
        this.fqdn = fqdn;
    }

    public String getServer_ip() {
        return server_ip;
    }

    public void setServer_ip(String server_ip) {
        this.server_ip = server_ip;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public long getVsys_id() {
        return vsys_id;
    }

    public void setVsys_id(long vsys_id) {
        this.vsys_id = vsys_id;
    }

    public String getDevice_group() {
        return device_group;
    }

    public void setDevice_group(String device_group) {
        this.device_group = device_group;
    }

    public String getClient_ip() {
        return client_ip;
    }

    public void setClient_ip(String client_ip) {
        this.client_ip = client_ip;
    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public String getData_center() {
        return data_center;
    }

    public void setData_center(String data_center) {
        this.data_center = data_center;
    }

    public String getInternal_ip() {
        return internal_ip;
    }

    public void setInternal_ip(String internal_ip) {
        this.internal_ip = internal_ip;
    }

    public String getExternal_ip() {
        return external_ip;
    }

    public void setExternal_ip(String external_ip) {
        this.external_ip = external_ip;
    }

    public String getSubscriber_id() {
        return subscriber_id;
    }

    public void setSubscriber_id(String subscriber_id) {
        this.subscriber_id = subscriber_id;
    }

    public long getSessions() {
        return sessions;
    }

    public void setSessions(long sessions) {
        this.sessions = sessions;
    }

    public long getOut_bytes() {
        return out_bytes;
    }

    public void setOut_bytes(long out_bytes) {
        this.out_bytes = out_bytes;
    }

    public long getIn_bytes() {
        return in_bytes;
    }

    public void setIn_bytes(long in_bytes) {
        this.in_bytes = in_bytes;
    }

    public long getOut_pkts() {
        return out_pkts;
    }

    public void setOut_pkts(long out_pkts) {
        this.out_pkts = out_pkts;
    }

    public long getIn_pkts() {
        return in_pkts;
    }

    public void setIn_pkts(long in_pkts) {
        this.in_pkts = in_pkts;
    }

    public String getKey_by() {
        return key_by;
    }

    public void setKey_by(String key_by) {
        this.key_by = key_by;
    }

    public String getL4_protocol() {
        return l4_protocol;
    }

    public void setL4_protocol(String l4_protocol) {
        this.l4_protocol = l4_protocol;
    }

    @Override
    public int compareTo(packetResultEntity per) {
        if((this.out_pkts+this.in_pkts)>=(per.out_pkts+per.in_pkts)){
            return 1 ;
        }else{
            return -1 ;
        }
    }


 /*   @Override public Object clone() {
        PacketResultEntity obj;
        try {
            obj = (PacketResultEntity) super.clone();
        } catch (CloneNotSupportedException e) {
            obj = new PacketResultEntity(this.source,this.vsys_id,this.session_num,this.c2s_pkt_num,this.s2c_pkt_num,this.c2s_byte_num,
        this.s2c_byte_num,this.order_by,this.device_group,this.data_center,this.stat_time);
        }
        return obj;
    }
*/

}
