package com.ada.trackSimilar;

import java.io.Serializable;

public class SegmentMessage implements Serializable, Message {
    public TrackPoint p1;
    public TrackPoint p2;

    public SegmentMessage() {
    }

    public SegmentMessage(TrackPoint p1, TrackPoint p2) {
        this.p1 = p1;
        this.p2 = p2;
    }
    public Segment toSegment(){
        return new Segment(p1, p2);
    }

    @Override
    public long getTimeStamp(){
        return p2.timestamp;
    }
}
