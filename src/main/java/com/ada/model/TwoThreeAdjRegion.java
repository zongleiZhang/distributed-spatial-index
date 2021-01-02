package com.ada.model;

import com.ada.geometry.Rectangle;

public class TwoThreeAdjRegion extends TwoThreeData{
    public Rectangle region;
    public long timeStamp;

    public TwoThreeAdjRegion() {
    }

    public TwoThreeAdjRegion(Integer key, Rectangle region, long timeStamp) {
        super(key);
        this.region = region;
        this.timeStamp = timeStamp;
    }

}
