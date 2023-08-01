package org.huang.flink.entrypoint.appdemo;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.java.tuple.Tuple2;

public class Ta implements TimestampAssigner<Tuple2<String, Integer>> {
    @Override
    public long extractTimestamp(Tuple2<String, Integer> element, long recordTimestamp) {
        return element.f1;
    }
}