package com.ada.geometry;

import com.ada.common.ClassMct;

import java.text.DecimalFormat;
import java.util.*;

import static java.lang.Integer.parseInt;

public class TrackPoint extends Point implements TrackInfo, Message,Cloneable, Comparable<TrackPoint> {
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
		String[] dev;
		if ( value.contains("\t") ){
			dev = value.split("\t");
		}else {
			dev = value.split(",");
			if (dev.length > 3) {
				String[] devDev = dev[1].split(" ");
				List<String> list = new ArrayList<>();
				list.add(dev[0]);
				list.addAll(Arrays.asList(devDev));
				list.addAll(Arrays.asList(dev).subList(2, dev.length));
				dev = list.toArray(new String[0]);
			}
		}
		if(dev.length == 5 && dev[1].length() == 10) {
			try {
				//System.out.println(value);
				TID = parseInt(dev[0]);
				Calendar timestamp = Calendar.getInstance();
				timestamp.set(parseInt(dev[1].substring(0, 4)), parseInt(dev[1].substring(5, 7)) - 1,
						parseInt(dev[1].substring(8, 10)), parseInt(dev[2].substring(0, 2)),
						parseInt(dev[2].substring(3, 5)), parseInt(dev[2].substring(6, 8)));
				this.timestamp = (timestamp.getTimeInMillis()/1000L)*1000L;
				double lon = Double.valueOf(dev[3]);
				double lat = Double.valueOf(dev[4]);
				data = ClassMct.LBToXY(lat, lon);
				if( data[0]<0 || data[0]>200000000 || data[1]<0 || data[1]>200000000 ) {
					data[0] = 0;
				}
			}catch (Exception e) {
				data[0] = 0;
			}
		}
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public int getTID() {
		return TID;
	}

	public void setTID(int TID) {
		this.TID = TID;
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
		StringBuilder sBuffer = new StringBuilder();
		Calendar timestamp = Calendar.getInstance();
		timestamp.setTimeInMillis(this.timestamp);
		DecimalFormat df = new DecimalFormat("#.0000");
		sBuffer.append(TID).append(" ");
		sBuffer.append(df.format(data[0])).append(" ");
		sBuffer.append(df.format(data[1])).append(" ");
		sBuffer.append(this.timestamp).append(" ");
		sBuffer.append(timestamp.get(Calendar.YEAR)).append("-");
		sBuffer.append(timestamp.get(Calendar.MONTH)+1).append("-");
		sBuffer.append(timestamp.get(Calendar.DAY_OF_MONTH)).append(" ");
		sBuffer.append(timestamp.get(Calendar.HOUR_OF_DAY)).append(":");
		sBuffer.append(timestamp.get(Calendar.MINUTE)).append(":");
		sBuffer.append(timestamp.get(Calendar.SECOND));
		return sBuffer.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof TrackPoint)) return false;
		if (!super.equals(o)) return false;
		TrackPoint point = (TrackPoint) o;
		return getTimestamp() == point.getTimestamp() &&
				getTID() == point.getTID();
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), getTimestamp(), getTID());
	}

}

