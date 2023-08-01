package org.huang.flink.entrypoint.appdemo;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;

public class Ws implements WatermarkStrategy<Tuple2<String, Integer>> {
    @Override
    public WatermarkGenerator<Tuple2<String, Integer>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new Wg();
    }

    @Override
    public TimestampAssigner<Tuple2<String, Integer>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new Ta();
    }
}