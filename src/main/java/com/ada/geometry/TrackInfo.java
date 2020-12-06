package com.ada.geometry;

/**
 * 车辆信息类的上层接口，一个车辆信息对象需要有车辆ID，采样点时间timeStamp。
 */
public interface TrackInfo{
    public int obtainTID();

    public long obtainTimeStamp();

    public Message toMessage();
}
