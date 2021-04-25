package com.ada.GQ_QBS_function;

import com.ada.common.Constants;
import com.ada.geometry.Point;
import com.ada.geometry.Rectangle;
import com.ada.geometry.Segment;
import com.ada.geometry.TrackPoint;
import com.ada.model.common.input.InputItem;
import com.ada.model.common.input.QueryItem;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

public class ToInputItemFlatMap extends RichFlatMapFunction<String, InputItem> {
    private Map<Integer, TrackPoint> tidTPMap = new HashMap<>();
    private long preTime;
    private long queryCount;
    private int subTask;

    @Override
    public void flatMap(String value, Collector<InputItem> out){
        TrackPoint tp = new TrackPoint(value);
        TrackPoint p0 = tidTPMap.get(tp.TID);
        if (p0 != null){
            //过滤停止不动的轨迹段和距离跨度过大的轨迹段
            if ( !(Constants.isEqual(tp.data[0], p0.data[0]) && Constants.isEqual(tp.data[1], p0.data[1]) ||
                    Math.abs(p0.data[0] - tp.data[0]) > Constants.maxSegment || Math.abs(p0.data[1] - tp.data[1]) > Constants.maxSegment) ) {
                Segment segment = new Segment(p0, tp);
                out.collect(segment);
                if (queryCount%Constants.ratio == 0){
                    Point point = segment.p2;
                    Rectangle rect = new Rectangle(point.clone(), point.clone()).extendLength(Constants.radius);
                    out.collect(new QueryItem((queryCount/Constants.ratio)*Constants.inputPartition + subTask, segment.p2.timestamp, rect));
                }
                queryCount++;
            }
            tidTPMap.replace(tp.TID, tp);
        }else {
            tidTPMap.put(tp.TID, tp);
            //删除不活跃的轨迹
            if ( tp.timestamp - preTime > 60000 * 30 ){
                tidTPMap.entrySet().removeIf(entry -> tp.timestamp - entry.getValue().timestamp > 60000 * 30);
                preTime = tp.timestamp;
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        subTask = getRuntimeContext().getIndexOfThisSubtask();
        preTime = 0L;
        queryCount = 0L;
    }
}























