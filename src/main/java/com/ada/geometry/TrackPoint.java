package com.ada.geometry;


import com.ada.model.densityToGlobal.Density2GlobalElem;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;


@Getter
@Setter
public class TrackPoint extends Point implements TrackInfo, Message,Cloneable, Comparable<TrackPoint>, Serializable, Density2GlobalElem {
	public long timestamp;
	public int TID;

	public TrackPoint(){}

	public TrackPoint(double[] data, long timestamp, int TID) {
		super(data);
		this.timestamp = timestamp;
		this.TID = TID;
	}

	public TrackPoint(String value){
		super();
		String[] dev = value.split("\\t");
		TID = Integer.parseInt(dev[0]);
		timestamp = Long.parseLong(dev[1]);
		data[0] = Double.parseDouble(dev[2]);
		data[1] = Double.parseDouble(dev[3]);
	}


    public boolean isEmpty(){
		return data == null;
	}

	@Override
	public int obtainTID(){
		return TID;
	}

	@Override
	public long obtainTimeStamp(){
		return timestamp;
	}

	@Override
	public long getTimeStamp(){
		return timestamp;
	}

	@Override
	public TrackPoint toMessage(){
		return this;
	}

	@Override
	public int compareTo(TrackPoint o) {
		int res = Long.compare(this.timestamp, o.timestamp);
		if (res == 0)
			return Integer.compare(TID, o.TID);
		else
			return res;
	}

	@Override
	public TrackPoint clone() {
		return (TrackPoint) super.clone();
	}

	@Override
	public String toString() {
		DecimalFormat df = new DecimalFormat("#.0000");
		Date date = new Date(this.timestamp);
		SimpleDateFormat myFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return TID + " " +
				df.format(data[0]) + " " +
				df.format(data[1]) + " " +
				this.timestamp + " " +
				myFormat.format(date);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof TrackPoint)) return false;
		if (!super.equals(o)) return false;
		TrackPoint point = (TrackPoint) o;
		return getTimeStamp() == point.getTimeStamp() &&
				getTID() == point.getTID();
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), getTimeStamp(), getTID());
	}

	@Override
	public int getD2GKey() {
		return TID;
	}
}

