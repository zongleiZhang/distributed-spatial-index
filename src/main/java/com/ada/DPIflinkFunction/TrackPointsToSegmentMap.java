package com.ada.DPIflinkFunction;

import com.ada.common.Constants;
import com.ada.trackSimilar.Segment;
import com.ada.trackSimilar.TrackPoint;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TrackPointsToSegmentMap extends RichFlatMapFunction<TrackPoint, Segment> {
    private Map<Integer, TrackPoint> tIDTPMap = new HashMap<>();
    private long preTime = 0;
    private int count = 0;
    @Override
    public void flatMap(TrackPoint value, Collector<Segment> out) throws Exception {
        TrackPoint p0 = tIDTPMap.get(value.TID);
        if (p0 != null){
            if ( !((Constants.isEqual(p0.data[0],value.data[0]) && Constants.isEqual(p0.data[1],value.data[1])) ||
                    Math.abs(p0.data[0] - value.data[0]) > 400 || Math.abs(p0.data[1] - value.data[1]) > 400) ) {
                Segment segment = new Segment(p0, value);
                out.collect(segment);
                if (count == 40) {
                    count = 0;
                    Segment query = segment.clone();
                    query.data = null;
                    query.p1.timestamp = System.currentTimeMillis();
                    query.rect.extendMultiple(1.0);
                    out.collect(query);
                }
                count++;
            }
            tIDTPMap.replace(value.TID,value);
        }else {
            tIDTPMap.put(value.TID,value);
            if ( value.timestamp - preTime > 60000 * 21 ){
                List<Integer> discardID = new ArrayList<>();
                for (TrackPoint point : tIDTPMap.values()) {
                    if ( value.timestamp - point.timestamp > 60000 * 21 )
                        discardID.add(point.TID);
                }
                for (Integer key : discardID)
                    tIDTPMap.remove(key);
                preTime = value.timestamp;
            }
        }

    }
}
