package com.ada.DTflinkFunction;

import com.ada.common.Constants;
import com.ada.geometry.TrackPoint;
import org.apache.flink.api.common.functions.MapFunction;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.List;

public class MapToTrackPoint implements MapFunction<String, TrackPoint> {

    List<RoaringBitmap> bitmaps;
    int factor;
    int parallelism;

    public MapToTrackPoint(){
        bitmaps = new ArrayList<>(Constants.densityPartition);
        for (int i = 0; i < Constants.densityPartition; i++)
            bitmaps.add(new RoaringBitmap());
        factor = 0;
        this.parallelism = Constants.densityPartition-1;
    }

    @Override
    public TrackPoint map(String value) {
        TrackPoint point = new TrackPoint(value);
        point.key = -1;
        for (int i = 0; i < bitmaps.size(); i++) {
            if (bitmaps.get(i).contains(point.TID)) {
                point.key = i;
                break;
            }
        }
        if (point.key == -1){
            point.key = factor = (factor + 1) & parallelism;
            bitmaps.get(factor).add(point.TID);
        }
        return point;
    }
}
