package com.galaxy.tsg.selector;

import com.galaxy.tsg.pojo.TransformEntity;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;

public  class GroupBySelector implements KeySelector<TransformEntity, Tuple5<String, Long, String, String, String>> {

    public String key;

    public GroupBySelector(String key) {
        this.key = key;
    }

    @Override
    public Tuple5<String, Long, String, String, String> getKey(TransformEntity transformEntity) throws Exception {

        Tuple5<String, Long, String, String, String> tuple = null;
        transformEntity.setKey_by(key);
        switch (key) {
            case "client_ip":
                tuple = new Tuple5<>(transformEntity.getClient_ip(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                break;
            case "server_ip":
                tuple = new Tuple5<>(transformEntity.getServer_ip(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                break;
            case "internal_ip":
                tuple = new Tuple5<>(transformEntity.getInternal_ip(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                break;
            case "external_ip":
                tuple = new Tuple5<>(transformEntity.getExternal_ip(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                break;
            case "server_domain":
                tuple = new Tuple5<>(transformEntity.getDomain(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                break;
            case "subscriber_id":
                tuple = new Tuple5<>(transformEntity.getSubscriber_id(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                break;
            case "server_fqdn":
                tuple = new Tuple5<>(transformEntity.getFqdn(), transformEntity.getVsys_id(), transformEntity.getDevice_group(), transformEntity.getData_center(), transformEntity.getDevice_id());
                break;

            default:

        }
        return tuple;
    }
}
