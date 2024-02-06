package com.galaxy.tsg.function;

import com.galaxy.tsg.pojo.SessionEntity;
import com.galaxy.tsg.pojo.TransformEntity;
import com.geedgenetworks.utils.FormatUtils;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlatMapFunction implements org.apache.flink.api.common.functions.FlatMapFunction<SessionEntity, TransformEntity> {

    private static final Logger LOG = LoggerFactory.getLogger(FlatMapFunction.class);


    @Override
    public void flatMap(SessionEntity sessionEntity, Collector<TransformEntity> out) throws Exception {
        TransformEntity transformEntity = new TransformEntity();
        try {
            transformEntity.setServer_ip(sessionEntity.getServer_ip());
            transformEntity.setClient_ip(sessionEntity.getClient_ip());
            transformEntity.setSubscriber_id(sessionEntity.getSubscriber_id());
            transformEntity.setFqdn(sessionEntity.getServer_fqdn());

            if(sessionEntity.getHttp_host()!=null && !sessionEntity.getHttp_host().isEmpty()){
                transformEntity.setDomain(FormatUtils.getTopPrivateDomain(sessionEntity.getHttp_host()));
            }
            else if(sessionEntity.getSsl_sni()!=null && !sessionEntity.getSsl_sni().isEmpty()) {
                transformEntity.setDomain(FormatUtils.getTopPrivateDomain(sessionEntity.getSsl_sni()));
            }
            else if(sessionEntity.getDtls_sni()!=null && !sessionEntity.getDtls_sni().isEmpty()){
                transformEntity.setDomain(FormatUtils.getTopPrivateDomain(sessionEntity.getDtls_sni()));
            }
            if(sessionEntity.getQuic_sni()!=null && !sessionEntity.getQuic_sni().isEmpty()){
                transformEntity.setDomain(FormatUtils.getTopPrivateDomain(sessionEntity.getQuic_sni()));
            }
            if(transformEntity.getDomain()!=null && transformEntity.getDomain().contains("InternetDomain")){
                System.out.println(transformEntity.getDomain());
            }
            transformEntity.setDevice_group(sessionEntity.getDevice_group());
            transformEntity.setDevice_id(sessionEntity.getDevice_id());
            transformEntity.setData_center(sessionEntity.getData_center());
            transformEntity.setVsys_id(sessionEntity.getVsys_id());
            transformEntity.setTimestamp(sessionEntity.getRecv_time()/1000);
            transformEntity.setSessions(1);
//


            if ((8L & sessionEntity.getFlags()) == 8L) {

                transformEntity.setOut_bytes(sessionEntity.getSent_bytes());
                transformEntity.setOut_pkts(sessionEntity.getSent_pkts());
                transformEntity.setIn_bytes(sessionEntity.getReceived_bytes());
                transformEntity.setIn_pkts(sessionEntity.getReceived_pkts());

            } else {
                transformEntity.setOut_bytes(sessionEntity.getReceived_bytes());
                transformEntity.setOut_pkts(sessionEntity.getReceived_pkts());
                transformEntity.setIn_bytes(sessionEntity.getSent_bytes());
                transformEntity.setIn_pkts(sessionEntity.getSent_pkts());


            }
            if(sessionEntity.getFlags()>0) {

                if ((8L & sessionEntity.getFlags()) == 8L && (16L & sessionEntity.getFlags()) == 16L) {
                    transformEntity.setInternal_ip(sessionEntity.getServer_ip());
                    out.collect(transformEntity);
                    transformEntity.setInternal_ip(sessionEntity.getClient_ip());

                } else if ((8L & sessionEntity.getFlags()) == 8L && (16L & sessionEntity.getFlags()) != 16L) {
                    transformEntity.setInternal_ip(sessionEntity.getClient_ip());
                    transformEntity.setExternal_ip(sessionEntity.getServer_ip());
                } else if ((8L & sessionEntity.getFlags()) != 8L && (16L & sessionEntity.getFlags()) == 16L) {
                    transformEntity.setInternal_ip(sessionEntity.getServer_ip());
                    transformEntity.setExternal_ip(sessionEntity.getClient_ip());
                } else if ((8L & sessionEntity.getFlags()) != 8L && (16L & sessionEntity.getFlags()) != 16L) {
                    transformEntity.setExternal_ip(sessionEntity.getServer_ip());
                    out.collect(transformEntity);
                    transformEntity.setExternal_ip(sessionEntity.getClient_ip());

                }
            }
        } catch (Exception e) {
            LOG.error("Entity Parsing ERROR");
            transformEntity.setIfError(1);
        }
        out.collect(transformEntity);
    }
}
