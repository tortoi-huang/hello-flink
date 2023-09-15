package org.huang.flink.entrypoint.pressured;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class Rs implements ReduceFunction<Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> e, Tuple2<String, Integer> c) throws Exception {
        c.f1 = c.f1 + e.f1;
        return c;
    }
}