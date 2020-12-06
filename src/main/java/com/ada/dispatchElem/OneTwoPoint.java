package com.ada.dispatchElem;

import com.ada.common.Constants;
import com.ada.geometry.TrackPoint;

public class OneTwoPoint extends OneTwoData {
    public TrackPoint point;

    public OneTwoPoint() {}

    public OneTwoPoint(TrackPoint point) {
        super(point.TID% Constants.keyTIDPartition);
        this.point = point;
    }
}
