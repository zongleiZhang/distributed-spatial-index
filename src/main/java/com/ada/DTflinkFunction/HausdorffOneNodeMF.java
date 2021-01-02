package com.ada.DTflinkFunction;

import com.ada.QBSTree.RCDataNode;
import com.ada.QBSTree.RCtree;
import com.ada.common.Constants;
import com.ada.geometry.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;

public class HausdorffOneNodeMF extends RichFlatMapFunction<TrackPoint, String> {
    private RCtree<Segment> pointIndex;
    private RCtree<TrackHauOne> pruneIndex;
    private Map<Integer, TrackHauOne> trackMap;
    private Map<Integer, TrackPoint> singlePointMap;
    private LinkedList<Tuple2<Long, RoaringBitmap>> appendQueue;
    private Tuple2<Long,RoaringBitmap> curAppend;
    private long count0 = 0;
    private long count1 = 0;
    private long timeStamp = 0L;



    @Override
    public void flatMap(TrackPoint trackPoint, Collector<String> out) throws CloneNotSupportedException {
        long minus = trackPoint.timestamp - curAppend.f0;
        if (minus >= Constants.windowSize) {
//            if (!cacheCheck())
//                throw new IllegalArgumentException(Constants.logicWindow + " " + "cacheCheck error.");
            appendQueue.add(curAppend);
            long newWinStart = curAppend.f0 + (int)(minus/Constants.windowSize)*Constants.windowSize;
            curAppend = new Tuple2<>(newWinStart, new RoaringBitmap());
            Tuple2<Long,RoaringBitmap> head = appendQueue.element();
            long logicStartWin = trackPoint.timestamp - Constants.windowSize*Constants.logicWindow;
            if (head.f0 < logicStartWin) {
                windowSlide(head, logicStartWin);
//                if (!cacheCheck())
//                    throw new IllegalArgumentException(Constants.logicWindow + " " + "cacheCheck error.");
            }
//            if (count1 > 10)
//                compare();
            String builder = "logicWindow:" + Constants.logicWindow + " " +
                    "topK:" + Constants.topK + " " +
                    "t:" + Constants.t + " " +
                    "count1:" + count1 + " " +
                    "speed:" + Constants.df.format(count0 / ((System.currentTimeMillis() - timeStamp) / 1000.0));
//            System.out.println(builder);
            out.collect(builder);
            timeStamp = System.currentTimeMillis();
            count1++;
            count0 = 0;
        }

        curAppend.f1.add(trackPoint.TID);

        TrackHauOne track = getTrajectory(trackPoint);
        if (track == null)
            return;

        if (trackMap.size() > Constants.topK*Constants.KNum*2){ //正常计算
            if (track.trajectory.elems.size() == 1) { //新的轨迹
                Rectangle MBR = track.trajectory.elems.getFirst().rect.clone();
                Rectangle pruneArea = MBR.clone().extendLength(Constants.extend);
                DTConstants.newTrackCalculate(track, MBR, pruneArea, pointIndex, trackMap);
                track.rect = MBR.extendLength(track.threshold);
                //将裁剪区域插入索引中
                pruneIndex.insert(track);
                DTConstants.mayBeAnotherTopK(track, pruneIndex, trackMap, null);
            }else  //已有轨迹
                hasTrackCalculate(track);
        }else { //初始计算
            if(trackMap.size() == Constants.topK*Constants.KNum*2)
                DTConstants.initCalculate(new ArrayList<>(trackMap.values()),
                        new ArrayList<>(trackMap.values()), pruneIndex);
        }
        count0++;

    }

    private void compare() {
        Map<Integer, TrackHauOne> map = new HashMap<>();
        for (TrackHauOne track : trackMap.values()) {
            List<Integer> candidate = new ArrayList<>(trackMap.keySet());
            candidate.remove(track.trajectory.TID);
            TrackHauOne newTrackHauOne = new TrackHauOne(null, null,null, track.trajectory.elems, track.trajectory.TID,candidate,new HashMap<>());
            map.put(track.trajectory.TID, newTrackHauOne);
        }
        for (TrackHauOne track : map.values()) {
            Set<TrackHauOne> set = new HashSet<>(map.values());
            set.remove(track);
            for (TrackHauOne comparedTrack : set) {
                SimilarState state = track.getSimilarState(comparedTrack.trajectory.TID);
                if (state == null) {
                    state = Constants.getHausdorff(track.trajectory, comparedTrack.trajectory);
                    track.putRelatedInfo(state);
                    comparedTrack.putRelatedInfo(state);
                }
            }
        }
        for (TrackHauOne track : map.values()) {
            track.sortCandidateInfo();
            TrackHauOne trajectory = trackMap.get(track.trajectory.TID);
            for (int i = 0; i < Constants.topK; i++) {
                int s0 = track.candidateInfo.get(i);
                int s1 = trajectory.candidateInfo.get(i);
                if (track.getSimilarState(s0).compareTo(trajectory.getSimilarState(s1)) != 0) {
                    System.out.println(track.candidateInfo.subList(0, Constants.topK));
                    System.out.println(trajectory.candidateInfo.subList(0, Constants.topK));
                    throw new IllegalArgumentException(Constants.logicWindow + " " + "result error.");
                }
            }
        }
    }



    /**
     * 窗口滑动逻辑
     * @param head 滑出的tuple
     * @param logicStartWin 新窗口的开始时间
     */
    private void windowSlide(Tuple2<Long, RoaringBitmap> head, long logicStartWin) {
        appendQueue.poll();

        //整条轨迹滑出窗口的轨迹ID集
        Set<Integer> emptyTIDs = new HashSet<>();

        //有采样点滑出窗口的轨迹（但没有整条滑出），记录其ID和滑出的Segment
        Map<Integer, List<Segment>> removeElemMap = new HashMap<>();

        DTConstants.removeSegment(head.f1, logicStartWin, removeElemMap, emptyTIDs, pointIndex, trackMap);

        //记录无采样点滑出，但其topK结果可能发生变化的轨迹TID
        Set<Integer> pruneChangeTIDs = new HashSet<>();

        //记录轨迹滑出导致其候选轨迹集小于K的轨迹ID集
        Set<Integer> canSmallTIDs = new HashSet<>();

        //处理整条轨迹滑出窗口的轨迹
        for (Integer tid : emptyTIDs)
            DTConstants.dealAllSlideOutTracks(trackMap.remove(tid), head.f1, pruneChangeTIDs, emptyTIDs, canSmallTIDs, trackMap, null,pruneIndex);

        //处理整条轨迹未完全滑出窗口的轨迹
        DTConstants.trackSlideOut(removeElemMap, pruneChangeTIDs, canSmallTIDs,trackMap,null,pruneIndex,pointIndex);

        pruneChangeTIDs.removeAll(canSmallTIDs);
        for (Integer tid : pruneChangeTIDs) {
            if (tid == 2563)
                System.out.print("");
            DTConstants.changeThreshold(trackMap.get(tid), -1, null, pruneIndex, pointIndex, trackMap);
        }
        for (Integer tid : canSmallTIDs) {
            if (tid == 2563)
                System.out.print("");
            DTConstants.dealCandidateSmall(removeElemMap, tid, null, trackMap,null,pruneIndex,pointIndex);
        }

        for (Integer tid : removeElemMap.keySet())
            DTConstants.mayBeAnotherTopK(trackMap.get(tid), pruneIndex, trackMap, null);
    }





    private TrackHauOne getTrajectory(TrackPoint trackPoint) throws CloneNotSupportedException {
        TrackHauOne track = trackMap.get(trackPoint.TID);
        Segment segment;
        if (track == null){
            TrackPoint prePoint = singlePointMap.get(trackPoint.TID);
            if (prePoint == null){
                singlePointMap.put(trackPoint.TID, trackPoint);
                return null;
            }else {
                singlePointMap.remove(trackPoint.TID);
                segment = new Segment(prePoint, trackPoint);
                track = new TrackHauOne(null,
                        segment.data.clone(),
                        segment.rect.clone(),
                        new LinkedList<>(Collections.singletonList(segment)),
                        trackPoint.TID,
                        new ArrayList<>(),
                        new HashMap<>());
                trackMap.put(trackPoint.TID, track);
            }
        }else {
            segment = new Segment(track.trajectory.elems.getLast().p2, trackPoint);
            track.trajectory.addElem(segment);
        }
        pointIndex.insert(segment);
        return track;
    }

    private void hasTrackCalculate(TrackHauOne track) throws CloneNotSupportedException {
        Segment newSegment = track.trajectory.elems.getLast();
        Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
        MBR = MBR.getUnionRectangle(newSegment.rect);
        DTConstants.updateTrackRelated(newSegment, track,trackMap, null, pruneIndex, pointIndex);
        //重新做一次裁剪和距离计算
        track.sortCandidateInfo();
        Rectangle pruneArea = DTConstants.recalculateTrackTopK(track, MBR,pointIndex,trackMap);
        pruneIndex.alterELem(track, pruneArea);
    }




    private boolean cacheCheck() {
        if (pruneIndex.root.elemNum != trackMap.size())
            throw new IllegalArgumentException(Constants.logicWindow + " " + "pruneIndex.root.elemNum != trackMap.size()");
        pruneIndex.check(null);
        pointIndex.check(null);
        for (TrackHauOne track : trackMap.values()) {
            if (!trackCheck(track))
                return false;
        }
        return true;
    }

    private boolean trackCheck(TrackHauOne track){
        if(!track.leaf.elms.contains(track)) //pruneIndex检查
            throw new IllegalArgumentException(track.trajectory.obtainTID() + "");
        for (Segment trackPoint : track.trajectory.elems) { //pointIndex检查
            if (!trackPoint.leaf.elms.contains(trackPoint))
                throw new IllegalArgumentException(track.trajectory.obtainTID() + " " + trackPoint.p1);
        }
        for (SimilarState key : track.getRelatedInfo().keySet()) { //state检查
            SimilarState state = track.getRelatedInfo().get(key);
            if (key != state)
                throw new IllegalArgumentException(state.comparingTID + " " + state.comparedTID);
            int comparedTid = Constants.getStateAnoTID(state, track.trajectory.TID);
            TrackHauOne comparedTrack = trackMap.get(comparedTid);
            SimilarState state1 = Constants.getHausdorff(track.trajectory, comparedTrack.trajectory);
            if(!Constants.isEqual(state.distance, state1.distance))
                throw new IllegalArgumentException(state.comparingTID + " " + state.comparedTID +
                        " " + state.distance + " " + state1.distance);
            if (!comparedTrack.candidateInfo.contains(track.trajectory.TID) && !track.candidateInfo.contains(comparedTid))
                throw new IllegalArgumentException(state.comparingTID + " " + state.comparedTID);
        }
        Rectangle rect = Constants.getPruningRegion(track.trajectory, track.threshold);
        if (!rect.equals(track.rect))
            throw new IllegalArgumentException(track.trajectory.TID + " " + track.rect + " " + rect);
        if (track.candidateInfo.size() < Constants.topK)
            throw new IllegalArgumentException(track.trajectory.TID + " " + track.candidateInfo.size());
        for (Integer tid : track.candidateInfo) {
            if(track.getSimilarState(tid) == null)
                throw new IllegalArgumentException(track.trajectory.TID + " " + tid);
        }
        for (int i = 0; i < track.candidateInfo.size()-1; i++) {
            SimilarState state1 = track.getSimilarState(track.candidateInfo.get(i));
            SimilarState state2 = track.getSimilarState(track.candidateInfo.get(i+1));
            if (Double.compare(state1.distance,state2.distance) > 0)
                throw new IllegalArgumentException(track.trajectory.TID + " " + Constants.getStateAnoTID(state1, track.trajectory.TID)
                        + " " + Constants.getStateAnoTID(state2, track.trajectory.TID));
        }
        Set<Integer> yz = pointIndex.getRegionInternalTIDs(track.rect);
        yz.remove(track.trajectory.TID);
        for (Integer tid : yz) {
            if (!track.candidateInfo.contains(tid))
                throw new IllegalArgumentException(track.trajectory.TID + " " + yz + "\t" + track.candidateInfo);
        }
        return true;
    }


    @Override
    public void open(Configuration parameters)  {
        pointIndex = new RCtree<>(8, 1, 17, Constants.globalRegion, 0, true);
        ((RCDataNode) pointIndex.root).TIDs = new HashSet<>();
        pruneIndex = new RCtree<>(8, 1, 17, Constants.globalRegion, 0, false);
        trackMap = new HashMap<>();
        singlePointMap = new HashMap<>();
        appendQueue = new LinkedList<>();
        curAppend = new Tuple2<>(0L, new RoaringBitmap());
    }


}
