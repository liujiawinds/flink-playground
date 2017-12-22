package org.hansight.finkplayground.aggregate;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.hansight.finkplayground.utils.JsonHelper;
import org.junit.Test;

/**
 * Created by liujia on 2017/12/1.
 */
public class AggregateTest {


    @Test
    public void testCountAndSum() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        Tuple2<Integer, Integer> t1 = new Tuple2<>(1, 1);
        Tuple2<Integer, Integer> t2 = new Tuple2<>(2, 2);
        Tuple2<Integer, Integer> t3 = new Tuple2<>(3, 3);
        Tuple2<Integer, Integer> t4 = new Tuple2<>(4, 4);
        Tuple2<Integer, Integer> t5 = new Tuple2<>(4, 5);

        DataStreamSource<Tuple2<Integer, Integer>> stream = env.fromElements(t1, t2, t3, t4, t5);
        stream.keyBy(0)
                .timeWindow(Time.seconds(1))
                .aggregate(new Aggregate())
                .addSink(new SinkFunction<Accumulator>() {
                    @Override
                    public void invoke(Accumulator value) throws Exception {
                        System.out.println("========>>>>>>>>>>> "+ JsonHelper.toJson(value));
                    }
                });
        env.execute();
    }
}
