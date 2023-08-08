package org.huang.flink.common;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class SystemOutSinkFunction<T> implements SinkFunction<T> {
    @Override
    public void invoke(T value, Context context) throws Exception {
        System.out.println(value);
    }
}
