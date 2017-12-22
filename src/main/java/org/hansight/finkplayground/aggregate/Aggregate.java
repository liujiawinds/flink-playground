package org.hansight.finkplayground.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 *
 */
public class Aggregate implements AggregateFunction<Tuple2<Integer, Integer>, Accumulator, Accumulator> {

    private static final long serialVersionUID = 3355966737412029618L;

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator(0L, "", 0.0, 0);
    }

    @Override
    public Accumulator add(Tuple2<Integer, Integer> value, Accumulator acc) {
        acc.sum += value.f0;
        acc.count++;
        return acc;
    }

    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        a.count += b.count;
        a.sum += b.sum;
        return a;
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }
}
