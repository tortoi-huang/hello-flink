package org.huang.flink.entrypoint.appdemo;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple2;

public class Wg implements WatermarkGenerator<Tuple2<String, Integer>> {
    private long time = 0;

    @Override
    public void onEvent(Tuple2<String, Integer> event, long eventTimestamp, WatermarkOutput output) {
        time = event.f1 > time ? event.f1 : time;
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(time));
    }
}