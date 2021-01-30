package com.ada.model.globalToLocal;

import com.ada.common.ArrayQueue;
import com.ada.geometry.Point;
import com.ada.geometry.Rectangle;
import com.ada.geometry.Segment;
import com.ada.geometry.TrackPoint;
import com.ada.geometry.track.TrackHauOne;
import com.ada.geometry.track.Trajectory;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Getter
@Setter
public class G2LPoints implements G2LValue, Serializable {
    public List<TrackPoint> points;
    public int TID;

    public G2LPoints() {}

    public G2LPoints(List<TrackPoint> points) {
        this.points = points;
        this.TID = points.get(0).TID;
    }

    public TrackHauOne toTrackHauOne(){
        List<Segment> segments = Segment.pointsToSegments(points);
        Rectangle MBR = Rectangle.pointsMBR(points.toArray(new Point[0]));
        return new TrackHauOne(null,
                MBR.getCenter().data,
                MBR,
                new ArrayQueue<>(segments),
                TID,
                new ArrayList<>(),
                new HashMap<>());
    }

    public static G2LPoints toG2LPoints(Trajectory track){
        return new G2LPoints(new ArrayList<>(Segment.segmentsToPoints(track.elms)));
    }
}
