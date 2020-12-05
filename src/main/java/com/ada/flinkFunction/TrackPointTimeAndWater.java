package com.ada.flinkFunction;


import com.ada.trackSimilar.TrackPoint;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class TrackPointTimeAndWater implements AssignerWithPeriodicWatermarks<TrackPoint>{

	private long currentMaxTimestamp = 0;

	public TrackPointTimeAndWater(){}

	@Override
	public Watermark getCurrentWatermark() {
		return new Watermark(currentMaxTimestamp - 10);
	}

	@Override
	public long extractTimestamp(TrackPoint point, long previousElementTimestamp) {
		long timestamp;
		timestamp = point.timestamp;
		currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
		return timestamp;
	}
}
