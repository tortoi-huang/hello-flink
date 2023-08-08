package org.huang.flink.common;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class StringAndOneTuple2Map implements MapFunction<String, Tuple2<String, Integer>> {
    @Override
    public Tuple2<String, Integer> map(String e) {
        return Tuple2.of(e, 1);
    }
}