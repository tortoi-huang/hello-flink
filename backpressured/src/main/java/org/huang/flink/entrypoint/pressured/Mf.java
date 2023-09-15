package org.huang.flink.entrypoint.pressured;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class Mf implements MapFunction<String, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String value) throws Exception {
        return Tuple2.of(value, 1);
    }
}