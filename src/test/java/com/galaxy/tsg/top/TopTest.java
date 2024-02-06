package com.galaxy.tsg.top;

import com.alibaba.fastjson2.JSON;
import com.galaxy.tsg.function.FlatMapFunction;
import com.galaxy.tsg.pojo.SessionEntity;
import com.galaxy.tsg.pojo.TransformEntity;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class TopTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());




    @Test
    public void testIncrementPipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        String jsonString1 = "{\"client_ip\":\"192.168.1.1\",\"app\":\"app1\",\"recv_time\":0,\"server_fqdn\":\"tuplogpublic.bangcdn.net\",\"flags\":24,\"decoded_as\":\"HTTP\",\"server_ip\":\"192.168.2.1\",\"http_host\":\"tuplogpublic.bangcdn.net\",\"vsys_id\":1,\"device_group\":\"group1\",\"device_id\":\"device_id1\",\"subscriber_id\":\"subscriber_id1\",\"data_center\":\"data_center1\",\"sent_pkts\":1,\"received_pkts\":1,\"sent_bytes\":1,\"received_bytes\":1  }";
        String jsonString2 = "{\"client_ip\":\"192.168.1.2\",\"app\":\"app1\",\"recv_time\":0,\"server_fqdn\":\"tuplogpublic.bangcdn.net\",\"flags\":8,\"decoded_as\":\"HTTP\",\"server_ip\":\"192.168.2.2\",\"http_host\":\"tuplogpublic.bangcdn.net\",\"vsys_id\":1,\"device_group\":\"group1\",\"device_id\":\"device_id1\",\"subscriber_id\":\"subscriber_id1\",\"data_center\":\"data_center1\",\"sent_pkts\":2,\"received_pkts\":1,\"sent_bytes\":1,\"received_bytes\":1  }";
        String jsonString3 = "{\"client_ip\":\"192.168.1.3\",\"app\":\"app1\",\"recv_time\":0,\"server_fqdn\":\"tuplogpublic.bangcdn.net\",\"flags\":16,\"decoded_as\":\"HTTP\",\"server_ip\":\"192.168.2.3\",\"http_host\":\"tuplogpublic.bangcdn.net\",\"vsys_id\":1,\"device_group\":\"group1\",\"device_id\":\"device_id1\",\"subscriber_id\":\"subscriber_id1\",\"data_center\":\"data_center1\",\"sent_pkts\":3,\"received_pkts\":1,\"sent_bytes\":1,\"received_bytes\":1  }";
        String jsonString4 = "{\"client_ip\":\"192.168.1.4\",\"app\":\"app1\",\"recv_time\":0,\"server_fqdn\":\"tuplogpublic.bangcdn.net\",\"flags\":1,\"decoded_as\":\"HTTP\",\"server_ip\":\"192.168.2.4\",\"http_host\":\"tuplogpublic.bangcdn.net\",\"vsys_id\":1,\"device_group\":\"group1\",\"device_id\":\"device_id1\",\"subscriber_id\":\"subscriber_id1\",\"data_center\":\"data_center1\",\"sent_pkts\":4,\"received_pkts\":1,\"sent_bytes\":1,\"received_bytes\":1  }";


        // configure your test environment

        SessionEntity sessionEntity1 = JSON.parseObject(jsonString1, SessionEntity.class);
        SessionEntity sessionEntity2 = JSON.parseObject(jsonString2, SessionEntity.class);
        SessionEntity sessionEntity3 = JSON.parseObject(jsonString3, SessionEntity.class);
        SessionEntity sessionEntity4 = JSON.parseObject(jsonString4, SessionEntity.class);

        env.setParallelism(1);
        // values are collected in a static variable
        CollectSink.values.clear();


        ParameterTool serviceConfig = ParameterTool.fromPropertiesFile("src\\main\\resources\\common.properties");
        Configuration configurationService = serviceConfig.getConfiguration();
        // global register
        env.getConfig().setGlobalJobParameters(configurationService);


        // create a stream of custom elements and apply transformations
        env.fromElements(sessionEntity1,sessionEntity2,sessionEntity3,sessionEntity4)
                .flatMap(new FlatMapFunction())
                .addSink(new CollectSink());

        // execute
        env.execute();

        // verify your results
        assertEquals("192.168.2.1", CollectSink.values.get(0).getInternal_ip());
        assertEquals("192.168.1.1", CollectSink.values.get(1).getInternal_ip());
        assertNull(CollectSink.values.get(0).getExternal_ip());
        assertNull(CollectSink.values.get(1).getExternal_ip());


        assertEquals("192.168.1.2", CollectSink.values.get(2).getInternal_ip());
        assertEquals("192.168.2.2", CollectSink.values.get(2).getExternal_ip());

        assertEquals("192.168.2.3", CollectSink.values.get(3).getInternal_ip());
        assertEquals("192.168.1.3", CollectSink.values.get(3).getExternal_ip());

        assertEquals("192.168.2.4", CollectSink.values.get(4).getExternal_ip());
        assertEquals("192.168.1.4", CollectSink.values.get(5).getExternal_ip());
        assertNull(CollectSink.values.get(4).getInternal_ip());
        assertNull(CollectSink.values.get(5).getInternal_ip());
        assertEquals(6, CollectSink.values.size());

        assertEquals("bangcdn.net", CollectSink.values.get(0).getDomain());



    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<TransformEntity> {

        // must be static
        public static final List<TransformEntity> values = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(TransformEntity value, SinkFunction.Context context) throws Exception {
            values.add(value);
        }
    }



}
