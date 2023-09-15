package org.huang.flink.entrypoint.pressured;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple2;

public class Wg implements WatermarkGenerator<String> {
    private long time = 0;

    @Override
    public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
        time = Math.max(eventTimestamp, time);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(time));
    }
}