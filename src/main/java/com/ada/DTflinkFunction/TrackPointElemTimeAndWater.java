package com.ada.DTflinkFunction;


import com.ada.trackSimilar.TrackPointElem;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class TrackPointElemTimeAndWater
		implements AssignerWithPeriodicWatermarks<TrackPointElem>{

	private long currentMaxTimestamp = 0;

	public TrackPointElemTimeAndWater(){}

	@Override
	public Watermark getCurrentWatermark() {
		return new Watermark(currentMaxTimestamp - 10);
	}

	@Override
	public long extractTimestamp(TrackPointElem point, long previousElementTimestamp) {
		long timestamp;
		timestamp = point.timestamp;
		currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
		return timestamp;
	}
}
