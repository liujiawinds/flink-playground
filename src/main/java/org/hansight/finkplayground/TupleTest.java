package org.hansight.finkplayground;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.builder.Tuple2Builder;

/**
 * Created by liujia on 2017/11/10.
 */
public class TupleTest {

    public static void main(String[] args) {
        Tuple2[] builder = new Tuple2Builder<String, String>()
                .add("1", "2")
                .add("3", "4")
                .add("4", "4")
                .add("5", "4")
                .add("6", "4")
                .add("7", "4")
                .build();

        for (Tuple2 tuple2 : builder) {
            System.out.println(tuple2);
        }

    }
}
