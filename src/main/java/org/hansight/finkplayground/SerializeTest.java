package org.hansight.finkplayground;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;


/**
 * Created by liujia on 2017/11/8.
 */
public class SerializeTest {


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map<String, String> esConfigMap = new HashMap<>();
        esConfigMap.put("cluster.name", "es-jw-darpa");
        esConfigMap.put("bulk.flush.max.actions", "1");

        DataStreamSource<Tuple3<String, Date, Integer>> dataStream = env.fromElements(new Tuple3<>("1", new Date(), 1),
                new Tuple3<>("2", new Date(), 2));

        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("172.16.150.31"), 29300));

        dataStream
                .addSink(new ElasticsearchSink<>(esConfigMap, transportAddresses, new ElasticsearchSinkFunction<Tuple3<String, Date, Integer>>() {

                    IndexRequest createIndexRequest(Tuple3<String, Date, Integer> element) {
                        Map<String, String> map = new HashMap<>();
                        map.put("new_id", element.f0);
                        map.put("date", element.f1.toString());

                        return Requests.indexRequest()
                                .index("0000_s")
                                .type("logon")
                                .source(map);
                    }

                    @Override
                    public void process(Tuple3<String, Date, Integer> tupleWrapper, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(tupleWrapper));
                    }
                }));

        env.execute();
    }
}
