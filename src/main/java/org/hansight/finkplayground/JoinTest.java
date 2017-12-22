package org.hansight.finkplayground;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created by liujia on 2017/11/10.
 */
public class JoinTest {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> dataStream = env.fromElements(new Tuple2<>("1", 1),
                new Tuple2<>("2", 2), new Tuple2<>("2", 3));

        DataStreamSource<Tuple2<String, Integer>> dataStream1 = env.fromElements(new Tuple2<>("1", 2),
                new Tuple2<>("2", 3), new Tuple2<>("2", 4));

        dataStream.join(dataStream1).where(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).equalTo(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.milliseconds(1)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object join(Tuple2<String, Integer> tuple2, Tuple2<String, Integer> tuple22) throws Exception {
                        System.out.println(tuple2 + "<<============>>" + tuple22);
                        return null;
                    }
                });

        env.execute();
    }
}
