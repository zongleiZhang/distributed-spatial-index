package com.ada.geometry;

import com.ada.QBSTree.RectElem;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 点无序
 */
public class Segment extends RectElem implements TrackInfo, Serializable {
    public TrackPoint p1;
    public TrackPoint p2;

    public Segment(){}

    public Segment(TrackPoint p1, TrackPoint p2){
        super(p1,p2);
        if (p1.timestamp > p2.timestamp){
            this.p2 = p1;
            this.p1 = p2;
        }else {
            this.p1 = p1;
            this.p2 = p2;
        }
    }

    public int getTID(){
        return p1.TID;
    }

    public long getFirstTime(){
        return p1.timestamp;
    }

    public long getSecondTime(){
        return p2.timestamp;
    }

    public static List<Segment> pointsToSegments(List<TrackPoint> points){
        if (points.size() < 2)
            throw new IllegalArgumentException("points are too small.");
        List<Segment> segments = new ArrayList<>();
        TrackPoint p0 = points.get(0);
        for (int i = 1; i < points.size(); i++) {
            TrackPoint p1 = points.get(i);
            segments.add(new Segment(p0,p1));
            p0 = p1;
        }
        return segments;
    }

    public static List<TrackPoint> segmentsToPoints(List<Segment> segments){
        if (segments.isEmpty())
            return new ArrayList<>();
        List<TrackPoint> points = new ArrayList<>();
        for (Segment segment : segments)
            points.add(segment.p1);
        points.add(segments.get(segments.size()-1).p2);
        return points;
    }

    public static List<TrackPoint> segmentsToPoints(ArrayDeque<Segment> segments){
        if (segments.isEmpty())
            return new ArrayList<>();
        List<TrackPoint> points = new ArrayList<>();
        for (Segment segment : segments)
            points.add(segment.p1);
        points.add(segments.getLast().p2);
        return points;
    }

    @Override
    public int obtainTID(){
        return p2.TID;
    }


    @Override
    public long obtainTimeStamp(){
        return p1.timestamp;
    }

    @Override
    public SegmentMessage toMessage(){
        return new SegmentMessage(p1, p2);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Segment)) return false;
        Segment segment = (Segment) o;
        return p1.equals(segment.p1) &&
                p2.equals(segment.p2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(p1, p2);
    }

    @Override
    public Segment clone()  {
        Segment segment = (Segment) super.clone();
        segment.p1 =  p1.clone();
        segment.p2 =  p2.clone();
        return segment;
    }

}
