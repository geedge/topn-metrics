package com.galaxy.tsg.pojo;

public class TopUrlEntity implements Comparable<TopUrlEntity>{


    public long stat_time ;

    public String url ;

    public long vsys_id ;

    public long session_num;

    public long getStat_time() {
        return stat_time;
    }

    public void setStat_time(long stat_time) {
        this.stat_time = stat_time;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public long getVsys_id() {
        return vsys_id;
    }

    public void setVsys_id(long vsys_id) {
        this.vsys_id = vsys_id;
    }

    public long getSession_num() {
        return session_num;
    }

    public void setSession_num(long session_num) {
        this.session_num = session_num;
    }

    @Override
    public int compareTo(TopUrlEntity per) {
        if(this.session_num>=per.session_num){
            return 1 ;
        }else{
            return -1 ;
        }
    }
}
