package org.huang.flink.common;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class Tuple2Feild1KeySelector<F0, F1> implements KeySelector<Tuple2<F0, F1>, F0> {
    @Override
    public F0 getKey(Tuple2<F0, F1> value) throws Exception {
        return value.f0;
    }
}
