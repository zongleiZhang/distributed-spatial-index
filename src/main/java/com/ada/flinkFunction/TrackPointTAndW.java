package com.ada.flinkFunction;

import com.ada.geometry.TrackPoint;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class TrackPointTAndW implements AssignerWithPeriodicWatermarks<TrackPoint> {
    private long maxTimestamp = 0;

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTimestamp -1);
    }

    @Override
    public long extractTimestamp(TrackPoint element, long previousElementTimestamp) {
        maxTimestamp = Math.max(element.timestamp, maxTimestamp);
        return element.timestamp;
    }
}
