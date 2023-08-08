package org.huang.flink.common;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class Sentence2wordsFlMap implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> out) {
        String[] split = value.split("[\\s,.]");
        for (String s : split) {
            out.collect(s);
        }
    }
}
