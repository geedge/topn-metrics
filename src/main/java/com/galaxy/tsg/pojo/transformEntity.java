package com.galaxy.tsg.pojo;

import java.io.Serializable;

public class transformEntity implements Serializable {

    public int ifError;
    public long timestamp;


    public String fqdn;
    public String server_ip ;
    public String domain ;
    public long vsys_id ;
    public String device_group ;
    public String client_ip ;
    public String device_id;
    public String data_center;
    public String internal_ip;
    public String external_ip;
    public String subscriber_id;
    public long sessions;
    public long out_bytes;
    public long in_bytes;
    public long out_pkts ;
    public long in_pkts ;
    public String key_by;
    public String l4_protocol;

    public transformEntity() {
    }

    public String getL4_protocol() {
        return l4_protocol;
    }

    public void setL4_protocol(String l4_protocol) {
        this.l4_protocol = l4_protocol;
    }

    public int getIfError() {
        return ifError;
    }

    public void setIfError(int ifError) {
        this.ifError = ifError;
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
}
