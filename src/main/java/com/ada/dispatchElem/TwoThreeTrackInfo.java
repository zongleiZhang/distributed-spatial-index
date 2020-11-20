package com.ada.dispatchElem;

import com.ada.trackSimilar.Message;

public class TwoThreeTrackInfo extends TwoThreeData{
    /**
     * 0:  添加经过点         (TrackPoint)
     * 1:  添加topK点         (TrackPoint)
     * 2： 新增经过轨迹   (TrackMessage)
     * 3： 新增topK轨迹   (TrackMessage)
     * 4： 删除经过轨迹   (TrackPoint)
     * 5： 删除topK轨迹   (TrackPoint)
     * 6： (调整负责区域)经过轨迹改为topK轨迹   (TrackPoint)
     * 7： (调整负责区域)topK轨迹改为经过轨迹   (TrackPoint)
     * 8： (调整负责区域)删除经过轨迹   (TrackPoint)
     * 9： (调整负责区域)删除topK轨迹   (TrackPoint)
     * 10：(调整负责区域)新增经过轨迹   (TrackMessage)
     * 11：(调整负责区域)新增topK轨迹   (TrackMessage)
     */
    public byte flag;

    public Message message;

    public TwoThreeTrackInfo() {
    }

    public TwoThreeTrackInfo(Integer key, byte flag, Message message) {
        super(key);
        this.flag = flag;
        this.message = message;
    }
}
