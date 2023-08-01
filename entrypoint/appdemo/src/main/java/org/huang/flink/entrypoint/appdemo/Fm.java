package org.huang.flink.entrypoint.appdemo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Fm implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String e, Collector<Tuple2<String, Integer>> c) throws Exception {
        String[] split = e.split("[\\s,.]");
        for (int i = 0; i < split.length; i++) {
            c.collect(Tuple2.of(split[i], i + 5000));
        }
    }
}