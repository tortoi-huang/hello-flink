package org.huang.flink.entrypoint.pressured;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple2;

public class Ws implements WatermarkStrategy<String> {
    @Override
    public WatermarkGenerator<String> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new Wg();
    }
}