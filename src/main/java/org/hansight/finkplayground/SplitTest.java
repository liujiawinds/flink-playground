package org.hansight.finkplayground;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liujia on 2017/11/10.
 */
public class SplitTest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> dataStream = env.fromElements(1, 2, 3);

        dataStream
                .map(i->i+1)
                .addSink(new SinkFunction<Integer>() {
                    @Override
                    public void invoke(Integer value) throws Exception {
                        System.out.println("output 1: "+value);
                    }
                }).name("output 1");

        dataStream
                .map(i->i+1000)
                .addSink(new SinkFunction<Integer>() {
                    @Override
                    public void invoke(Integer value) throws Exception {
                        System.out.println("output 2: "+value);
                    }
                })
                .name("output 2");

        SplitStream<Integer> split = dataStream.split(new OutputSelector<Integer>() {

            @Override
            public Iterable<String> select(Integer value) {
                List<String> ret = new ArrayList<String>();
                if (value % 2 == 0 ){
                    ret.add("odd");
                } else{
                    ret.add("even");
                }
                return ret;
            }
        });
        split.select("even").addSink(new SinkFunction<Integer>() {
            @Override
            public void invoke(Integer value) throws Exception {
                System.out.println("after split: "+value);
            }
        }).name("split");

        env.execute();
    }
}
