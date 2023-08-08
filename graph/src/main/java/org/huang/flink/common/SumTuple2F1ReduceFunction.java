package org.huang.flink.common;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class SumTuple2F1ReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
    }
}
