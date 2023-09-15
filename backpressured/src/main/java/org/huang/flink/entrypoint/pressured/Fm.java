package org.huang.flink.entrypoint.pressured;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Fm implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String e, Collector<String> c) throws Exception {
        String[] split = e.split("[\\s,.]");
        for (String s: split) {
            c.collect(s);
        }
    }
}