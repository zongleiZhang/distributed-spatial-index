package com.ada.flinkFunction;

import com.ada.common.Constants;
import com.ada.trackSimilar.Segment;
import com.ada.trackSimilar.TrackPoint;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class TrackPointsToSegmentMap extends RichFlatMapFunction<TrackPoint, Segment> {
    private Map<Integer, TrackPoint> tidTPMap = new HashMap<>();
    private long preTime = 0;
    private int count = 0;
    @Override
    public void flatMap(TrackPoint value, Collector<Segment> out){
        TrackPoint p0 = tidTPMap.get(value.TID);
        if (p0 != null){
            //过滤停止不动的轨迹段和距离跨度过大的轨迹段
            if ( !(Constants.isEqual(value.data[0], p0.data[0]) && Constants.isEqual(value.data[1], p0.data[1]) ||
                    Math.abs(p0.data[0] - value.data[0]) > 400 || Math.abs(p0.data[1] - value.data[1]) > 400) ) {
                Segment segment = new Segment(p0, value);
                out.collect(segment);

                //产生索引的查询项
                if (count == 40) {
                    count = 0;
                    Segment query = segment.clone();

                    //查询项的标志，即data为null，p1.timestamp为当前系统当前时间戳。
                    query.data = null;
                    query.p1.timestamp = System.currentTimeMillis();

                    //设置查询范围
                    query.rect.extendMultiple(1.0);
                    out.collect(query);
                }
                count++;
            }
            tidTPMap.replace(value.TID,value);
        }else {
            tidTPMap.put(value.TID,value);
            //删除不活跃的轨迹
            if ( value.timestamp - preTime > 60000 * 30 ){
                tidTPMap.entrySet().removeIf(entry -> value.timestamp - entry.getValue().timestamp > 60000 * 30);
                preTime = value.timestamp;
            }
        }
    }
}
