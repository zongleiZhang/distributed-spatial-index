package com.ada.DTflinkFunction;


import com.ada.QBSTree.DualRootTree;
import com.ada.QBSTree.RCtree;
import com.ada.common.Constants;
import com.ada.geometry.Rectangle;
import com.ada.geometry.Segment;
import com.ada.geometry.track.TrackHauOne;
import com.ada.model.globalToLocal.Global2LocalElem;
import com.ada.model.globalToLocal.Global2LocalPoints;
import com.ada.model.globalToLocal.Global2LocalTID;
import com.ada.model.globalToLocal.Global2LocalValue;
import com.ada.model.queryResult.QueryResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;

public class HausdorffLocalPF extends ProcessWindowFunction<Global2LocalElem, QueryResult, Integer, TimeWindow> {
    private int subTask;
    private boolean hasInit;
    private Collector<QueryResult> out;
    private int count;

    private Map<Long, RoaringBitmap> tIDsMap;
    private Map<Integer, TrackHauOne> passTrackMap;
    private Map<Integer, TrackHauOne> topKTrackMap;
    private DualRootTree<Segment> segmentIndex;
    private DualRootTree<TrackHauOne> pruneIndex;

    List<Global2LocalPoints> addPassPoints; //0
    List<Global2LocalPoints> addTopKPoints; //1
    List<Global2LocalPoints> addPassTracks; //2
    List<Global2LocalPoints> addTopKTracks; //3
    List<Integer> delPassTIDs;  //4
    List<Integer> delTopKTIDs;  //5
    List<Integer> convertPassTIDs;  //6
    List<Integer> convertTopKTIDs;  //7
    List<Global2LocalValue> adjustInfo; //8-13
    Rectangle newRegion;    //14


    @Override
    public void process(Integer key, Context context, Iterable<Global2LocalElem> elements, Collector<QueryResult> out) throws Exception {
        preElements(elements);
        if(!hasInit){
            if (newRegion == null)
                throw new NullPointerException("newRegion is null");
            hasInit = true;
            tIDsMap = new HashMap<>();
            passTrackMap = new HashMap<>();
            topKTrackMap = new HashMap<>();
            segmentIndex = new DualRootTree<>(4,1,11, newRegion.clone().extendMultiple(0.1), Constants.globalRegion, true);
            pruneIndex  = new DualRootTree<>(4,1,11, newRegion.clone().extendMultiple(0.1), Constants.globalRegion, true);
        }

    }

    private void preElements(Iterable<Global2LocalElem> elements) {
        for (Global2LocalElem element : elements) {
            switch (element.flag){
                case 0:
                    addPassPoints.add((Global2LocalPoints) element.value);
                    break;
                case 1:
                    addTopKPoints.add((Global2LocalPoints) element.value);
                    break;
                case 2:
                    addPassTracks.add((Global2LocalPoints) element.value);
                    break;
                case 3:
                    addTopKTracks.add((Global2LocalPoints) element.value);
                    break;
                case 4:
                    delPassTIDs.add(((Global2LocalTID) element.value).TID);
                    break;
                case 5:
                    delTopKTIDs.add(((Global2LocalTID) element.value).TID);
                    break;
                case 6:
                    convertPassTIDs.add(((Global2LocalTID) element.value).TID);
                    break;
                case 7:
                    convertTopKTIDs.add(((Global2LocalTID) element.value).TID);
                    break;
                case 14:
                    newRegion = (Rectangle) element.value;
                    break;
                default:
                    adjustInfo.add(element.value);
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        subTask = getRuntimeContext().getIndexOfThisSubtask();
        hasInit = false;
    }

}
