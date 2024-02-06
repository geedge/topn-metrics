package com.galaxy.tsg.pojo;

import java.io.Serializable;

public class SessionEntity implements Serializable{

    public String client_ip ;
    public String app ;
    public long recv_time ;
    public String server_fqdn;
    public long flags;
    public String decoded_as ;
    public String server_ip ;
    public String http_host ;
    public String ssl_sni ;
    public String dtls_sni ;
    public String quic_sni ;
    public long vsys_id ;
    public String device_group ;
    public String device_id ;
    public String data_center;

    public String subscriber_id;
    public long sent_pkts;
    public long received_pkts;
    public long sent_bytes ;
    public long received_bytes;


    public SessionEntity() {
    }

    public String getDevice_id() {
        return device_id;
    }

    public void setDevice_id(String device_id) {
        this.device_id = device_id;
    }

    public String getClient_ip() {
        return client_ip;
    }

    public void setClient_ip(String client_ip) {
        this.client_ip = client_ip;
    }

    public String getApp() {
        return app;
    }

    public void setApp(String common_app_label) {
        this.app = app;
    }

    public long getRecv_time() {
        return recv_time;
    }

    public void setRecv_time(long recv_time) {
        this.recv_time = recv_time;
    }

    public String getDecoded_as() {
        return decoded_as;
    }

    public void setDecoded_as(String decoded_as) {
        this.decoded_as = decoded_as;
    }

    public String getServer_ip() {
        return server_ip;
    }

    public void setServer_ip(String server_ip) {
        this.server_ip = server_ip;
    }


    public String getHttp_host() {
        return http_host;
    }

    public void setHttp_host(String http_host) {
        this.http_host = http_host;
    }

    public String getSsl_sni() {
        return ssl_sni;
    }

    public void setSsl_sni(String ssl_sni) {
        this.ssl_sni = ssl_sni;
    }

    public String getDtls_sni() {
        return dtls_sni;
    }

    public void setDtls_sni(String dtls_sni) {
        this.dtls_sni = dtls_sni;
    }

    public String getQuic_sni() {
        return quic_sni;
    }

    public void setQuic_sni(String quic_sni) {
        this.quic_sni = quic_sni;
    }

    public long getVsys_id() {
        return vsys_id;
    }

    public void setVsys_id(long common_vsys_id) {
        this.vsys_id = vsys_id;
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



    public String getSubscriber_id() {
        return subscriber_id;
    }

    public void setSubscriber_id(String subscriber_id) {
        this.subscriber_id = subscriber_id;
    }


    public long getSent_pkts() {
        return sent_pkts;
    }

    public void setSent_pkts(long sent_pkts) {
        this.sent_pkts = sent_pkts;
    }

    public long getReceived_pkts() {
        return received_pkts;
    }

    public void setReceived_pkts(long received_pkts) {
        this.received_pkts = received_pkts;
    }

    public long getSent_bytes() {
        return sent_bytes;
    }

    public void setSent_bytes(long sent_bytes) {
        this.sent_bytes = sent_bytes;
    }

    public long getReceived_bytes() {
        return received_bytes;
    }

    public void setReceived_bytes(long received_bytes) {
        this.received_bytes = received_bytes;
    }


    public String getServer_fqdn() {
        return server_fqdn;
    }

    public void setServer_fqdn(String server_fqdn) {
        this.server_fqdn = server_fqdn;
    }

    public long getFlags() {
        return flags;
    }

    public void setFlags(long flags) {
        this.flags = flags;
    }


}
