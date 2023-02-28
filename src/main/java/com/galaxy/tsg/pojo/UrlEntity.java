package com.galaxy.tsg.pojo;

public class UrlEntity {


    public long common_recv_time ;

    public String http_url ;

    public long common_vsys_id ;

    public long common_sessions;

    public int ifError;

    public int getIfError() {
        return ifError;
    }

    public void setIfError(int ifError) {
        this.ifError = ifError;
    }

    public long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_recv_time = common_recv_time;
    }

    public String getHttp_url() {
        return http_url;
    }

    public void setHttp_url(String http_url) {
        this.http_url = http_url;
    }

    public long getCommon_vsys_id() {
        return common_vsys_id;
    }

    public void setCommon_vsys_id(long common_vsys_id) {
        this.common_vsys_id = common_vsys_id;
    }

    public long getCommon_sessions() {
        return common_sessions;
    }

    public void setCommon_sessions(long common_sessions) {
        this.common_sessions = common_sessions;
    }
}
