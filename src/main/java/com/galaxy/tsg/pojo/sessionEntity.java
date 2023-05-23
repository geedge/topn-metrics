package com.galaxy.tsg.pojo;

import java.io.Serializable;

public class sessionEntity implements Serializable {

    public String common_client_ip ;
    public String common_app_label ;
    public long common_recv_time ;
    public String common_server_fqdn;
    public long common_flags;
    public String common_schema_type ;
    public String common_server_ip ;
    public String http_domain ;
    public long common_vsys_id ;
    public String common_device_group ;
    public String common_device_id ;
    public String common_data_center;
    public String common_l4_protocol;
    public String common_internal_ip;
    public String common_external_ip;
    public String common_subscriber_id;
    public long common_sessions;
    public long common_c2s_pkt_num;
    public long common_s2c_pkt_num;
    public long common_c2s_byte_num ;
    public long common_s2c_byte_num ;


    public sessionEntity() {
    }

    public String getCommon_device_id() {
        return common_device_id;
    }

    public void setCommon_device_id(String common_device_id) {
        this.common_device_id = common_device_id;
    }

    public String getCommon_client_ip() {
        return common_client_ip;
    }

    public void setCommon_client_ip(String common_client_ip) {
        this.common_client_ip = common_client_ip;
    }

    public String getCommon_app_label() {
        return common_app_label;
    }

    public void setCommon_app_label(String common_app_label) {
        this.common_app_label = common_app_label;
    }

    public long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_recv_time = common_recv_time;
    }

    public String getCommon_schema_type() {
        return common_schema_type;
    }

    public void setCommon_schema_type(String common_schema_type) {
        this.common_schema_type = common_schema_type;
    }

    public String getCommon_server_ip() {
        return common_server_ip;
    }

    public void setCommon_server_ip(String common_server_ip) {
        this.common_server_ip = common_server_ip;
    }



    public String getHttp_domain() {
        return http_domain;
    }

    public void setHttp_domain(String http_domain) {
        this.http_domain = http_domain;
    }

    public long getCommon_vsys_id() {
        return common_vsys_id;
    }

    public void setCommon_vsys_id(long common_vsys_id) {
        this.common_vsys_id = common_vsys_id;
    }

    public String getCommon_device_group() {
        return common_device_group;
    }

    public void setCommon_device_group(String common_device_group) {
        this.common_device_group = common_device_group;
    }


    public String getCommon_data_center() {
        return common_data_center;
    }

    public void setCommon_data_center(String common_data_center) {
        this.common_data_center = common_data_center;
    }

    public String getCommon_l4_protocol() {
        return common_l4_protocol;
    }

    public void setCommon_l4_protocol(String common_l4_protocol) {
        this.common_l4_protocol = common_l4_protocol;
    }

    public String getCommon_internal_ip() {
        return common_internal_ip;
    }

    public void setCommon_internal_ip(String common_internal_ip) {
        this.common_internal_ip = common_internal_ip;
    }

    public String getCommon_external_ip() {
        return common_external_ip;
    }

    public void setCommon_external_ip(String common_external_ip) {
        this.common_external_ip = common_external_ip;
    }

    public String getCommon_subscriber_id() {
        return common_subscriber_id;
    }

    public void setCommon_subscriber_id(String common_subscriber_id) {
        this.common_subscriber_id = common_subscriber_id;
    }

    public long getCommon_sessions() {
        return common_sessions;
    }

    public void setCommon_sessions(long common_sessions) {
        this.common_sessions = common_sessions;
    }

    public long getCommon_c2s_pkt_num() {
        return common_c2s_pkt_num;
    }

    public void setCommon_c2s_pkt_num(long common_c2s_pkt_num) {
        this.common_c2s_pkt_num = common_c2s_pkt_num;
    }

    public long getCommon_s2c_pkt_num() {
        return common_s2c_pkt_num;
    }

    public void setCommon_s2c_pkt_num(long common_s2c_pkt_num) {
        this.common_s2c_pkt_num = common_s2c_pkt_num;
    }

    public long getCommon_c2s_byte_num() {
        return common_c2s_byte_num;
    }

    public void setCommon_c2s_byte_num(long common_c2s_byte_num) {
        this.common_c2s_byte_num = common_c2s_byte_num;
    }

    public long getCommon_s2c_byte_num() {
        return common_s2c_byte_num;
    }

    public void setCommon_s2c_byte_num(long common_s2c_byte_num) {
        this.common_s2c_byte_num = common_s2c_byte_num;
    }


    public String getCommon_server_fqdn() {
        return common_server_fqdn;
    }

    public void setCommon_server_fqdn(String common_server_fqdn) {
        this.common_server_fqdn = common_server_fqdn;
    }

    public long getCommon_flags() {
        return common_flags;
    }

    public void setCommon_flags(long common_flags) {
        this.common_flags = common_flags;
    }
}
