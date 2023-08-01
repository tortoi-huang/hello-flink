package org.huang.flink.entrypoint.appdemo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class Mf implements MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
        return Tuple2.of(value.f0, 1);
    }
}