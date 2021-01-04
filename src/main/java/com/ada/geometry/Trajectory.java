package com.ada.geometry;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.*;

@Getter
@Setter
public class Trajectory implements Serializable, TrackInfo {
    public ArrayDeque<Segment> elms;
    public Integer TID;


    public Trajectory(ArrayDeque<Segment> elms,
                      int TID){
        this.elms = elms;
        this.TID = TID;
    }


    public void addSegment(Segment segment) {
        elms.add(segment);
    }

    public void addSegments(List<Segment> value) {
        elms.addAll(value);
    }

    /**
     * 移除过时采样点
     * @param time 时间标准
     * @return 0 本轨迹的所有采样点都过时， 1移除过时采样点后还有超过一个元素保留
     */
    public List<Segment> removeElem(long time){
        List<Segment> timeOutElem = new ArrayList<>();
        while(!elms.isEmpty() && elms.element().p1.getTimeStamp() < time) {
            timeOutElem.add(elms.remove());
        }
        return timeOutElem;
    }


    @Override
    public TrackMessage toMessage(){
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Trajectory)) return false;
        Trajectory that = (Trajectory) o;
        return TID.equals(that.TID);
    }

    @Override
    public int hashCode() {
        return TID.hashCode();
    }

    @Override
    public int obtainTID() {
        return TID;
    }

    @Override
    public long obtainTimeStamp() {
        return elms.element().obtainTimeStamp();
    }
}















