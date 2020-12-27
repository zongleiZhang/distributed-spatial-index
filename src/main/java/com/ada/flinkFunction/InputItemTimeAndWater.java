package com.ada.flinkFunction;


import com.ada.model.inputItem.InputItem;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class InputItemTimeAndWater implements AssignerWithPeriodicWatermarks<InputItem>{

	private long currentMaxTimestamp = 0;

	public InputItemTimeAndWater(){}

	@Override
	public Watermark getCurrentWatermark() {
		return new Watermark(currentMaxTimestamp - 1);
	}

	@Override
	public long extractTimestamp(InputItem element, long previousElementTimestamp) {
		long timestamp = element.getTimeStamp();
		currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
		return timestamp;
	}
}
