package com.ada.model.globalToLocal;

import com.ada.common.ArrayQueue;
import com.ada.geometry.Segment;
import com.ada.geometry.TrackPoint;
import com.ada.geometry.track.Trajectory;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class Global2LocalPoints implements Global2LocalValue, Serializable {
    public List<TrackPoint> points;

    public Global2LocalPoints(List<TrackPoint> points) {
        this.points = points;
    }

    public Trajectory ToTrajectory(){
        return new Trajectory(new ArrayQueue<>(Segment.pointsToSegments(points)), points.get(0).TID);
    }

    public static Global2LocalPoints ToG2LPoints(Trajectory track){
        return new Global2LocalPoints(new ArrayList<>(Segment.segmentsToPoints(track.elms)));
    }
}
