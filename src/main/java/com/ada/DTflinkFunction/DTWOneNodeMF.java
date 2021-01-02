package com.ada.DTflinkFunction;

import com.ada.QBSTree.RCtree;
import com.ada.geometry.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class DTWOneNodeMF extends RichFlatMapFunction<TrackPointElem, String> {
    private RCtree<Segment> pointIndex;
//    private RCtree<Trajectory<Segment>> pruneIndex;
    private Map<Integer, Trajectory<Segment>> trackMap;
    private Map<Integer, TrackPoint> singlePointMap;
    private Queue<Tuple2<Long,Set<Integer>>> appendQueue;
    private Tuple2<Long,Set<Integer>> curAppend;
    private long count0 = 0;
    private long count1 = 0;

    @Override
    public void flatMap(TrackPointElem trackPoint, Collector<String> out) {
//        count0++;
//        if(count0 == 1000){
//            cacheCheck();
//            System.out.println(count1);
//            pruneIndex.check();
//            pointIndex.check();
//            count0 = 0;
//            count1++;
//        }



//        if (count1 == 7 && count0 == 90)
//            System.out.print("");
//
//        if (trackPoint.timestamp - curAppend.f0 > Constants.windowSize) {
//            appendQueue.add(curAppend);
//            long newWinStart = curAppend.f0 + (int)((trackPoint.timestamp - curAppend.f0)/Constants.windowSize)*Constants.windowSize;
//            curAppend = new Tuple2<>(newWinStart, new HashSet<>());
//            Tuple2<Long,Set<Integer>> head = appendQueue.element();
//            long startWindow = trackPoint.timestamp - Constants.windowSize*Constants.logicWindow;
//            if (head.f0 < startWindow) {
//                appendQueue.poll();
//                Map<Integer, List<Segment>> removeElemMap = new HashMap<>();
//                for (Integer tid : head.f1) {
//                    Trajectory<Segment> track = trackMap.get(tid);
//                    List<Segment> timeElems = track.removeElem(startWindow);
//                    removeElemMap.put(tid,timeElems);
//                }
//                removeElemMap.forEach((tid, timeOutElems) -> {
//                    Trajectory<Segment> track = trackMap.get(tid);
//                    if (track.elems.isEmpty()){
//                        trackMap.remove(tid);
//                        pruneIndex.delete(track);
//                        for (SimilarState state : track.relatedInfo.values()) {
//                            int comparedTid;
//                            if (state.comparingTID == tid)
//                                comparedTid = state.comparedTID;
//                            else
//                                comparedTid = state.comparingTID;
//                            Trajectory<Segment> comparedTrack = trackMap.get(comparedTid);
//                            int index = comparedTrack.candidateInfo.indexOf(tid);
//                            if (index > Constants.topK-1){
//                                comparedTrack.candidateInfo.remove(tid);
//                                comparedTrack.relatedInfo.remove(state);
//                            }else{
//                                if (index != -1){
//                                    if (head.f1.contains(comparedTid)){
//                                        comparedTrack.candidateInfo.set(index, -1);
//                                    }else {
//
//                                    }
//                                }
//                            }
//
//                        }
//
//                    }else {
//
//                    }
//                });
//
//
//            }
//        }
//
//        curAppend.f1.add(trackPoint.TID);
//
//        //Hausdorff
//        Segment segment;
//        Trajectory<Segment> track = trackMap.get(trackPoint.TID);
//        if (track == null){
//            TrackPoint prePoint = singlePointMap.get(trackPoint.TID);
//            if (prePoint == null){
//                singlePointMap.put(trackPoint.TID, trackPoint);
//                return;
//            }else {
//                singlePointMap.remove(trackPoint.TID);
//                segment = new Segment(prePoint, trackPoint);
//                track = new Trajectory<>(null,
//                        segment.data.clone(),
//                        segment.rect.clone(),
//                        new LinkedList<>(Collections.singletonList(segment)),
//                        trackPoint.TID,
//                        new ArrayList<>(),
//                        new HashMap<>());
//                trackMap.put(trackPoint.TID, track);
//            }
//        }else {
//            segment = new Segment(track.elems.getLast().p2, trackPoint);
//            track.addElem(segment);
//        }
//        pointIndex.insert(segment);
//
////        DTW
////        curAppend.f1.add(trackPoint.TID);
////        pointIndex.insert(trackPoint);
//
//
//        if (trackMap.size() > Constants.topK*Constants.KNum){ //正常计算
//            if (track.elems.size() == 1) //新的轨迹
//                newTrackCalculate(track);
//            else  //已有轨迹
//                hasTrackCalculate(segment, track);
//            //可能成为其他轨迹的新的topK结果
//            mayBeAnotherTopk(segment, track);
//        }else { //初始计算
//            if(trackMap.size() == Constants.topK*Constants.KNum)
//                initCalculate();
//        }
    }

//    private void hasTrackCalculate(Segment seg, Trajectory<Segment> track) throws CloneNotSupportedException {
//        double threshold = track.threshold;
//        Rectangle MBR = track.rect.clone().extendLength(-threshold);
//
//        List<Integer> removeRI = new ArrayList<>();
//        //更新新采样点所属的轨迹与相关轨迹的距离
//        for (SimilarState state : track.relatedInfo.values()) {
//            int comparedTid;
//            if (state.comparedTID == track.TID)
//                comparedTid = state.comparingTID;
//            else
//                comparedTid = state.comparedTID;
//            Trajectory<Segment> comparedTrack = trackMap.get(comparedTid);
//            if (!comparedTrack.candidateInfo.contains(track.TID) && !track.candidateInfo.contains(comparedTid))
//                throw new IllegalArgumentException("error state");
//            double oldThreshold = comparedTrack.calculateThreshold();
//            int oldIndex = comparedTrack.candidateInfo.indexOf(track.TID);
////            Constants.incrementDTW(Collections.singletonList(trackPoint), comparedTrack, state);
//            Constants.incrementHausdorff(Collections.singletonList(seg.p2), comparedTrack, state);
//            if (oldIndex != -1) {
//                int newIndex = comparedTrack.updateCandidateInfo(track.TID);
//                double newThreshold = comparedTrack.calculateThreshold();
//                //有更近的topK结果更新裁剪区域即可。
//                if ( (oldIndex == Constants.topK - 1 && oldThreshold > newThreshold) ||
//                        (oldIndex > Constants.topK - 1 && newIndex <= Constants.topK - 1) ) {
//                    Rectangle comparedPruning = comparedTrack.rect.clone().extendLength(newThreshold - oldThreshold);
//                    if (comparedTrack.threshold - newThreshold > 20.0) {
//                        if (cacheTighten(comparedTrack, comparedPruning, track.TID))
//                            removeRI.add(comparedTid);
//                        comparedTrack.threshold = newThreshold;
//                    }
//                    pruneIndex.alterELem(comparedTrack, comparedPruning);
//                }
//                //第K个结果变大，裁剪域变得不安全了，需要重新计算comparedTrack的topK结果。
//                if ( (oldIndex == Constants.topK - 1 && oldThreshold < newThreshold) ||
//                        (oldIndex < Constants.topK - 1 && newIndex >= Constants.topK - 1) ) {
//                    Rectangle comparedPruning = comparedTrack.rect.clone().extendLength(newThreshold - oldThreshold);
//                    Set<Integer> newCandidate = pointIndex.getRegionTIDs(comparedPruning, true);
//                    newCandidate.remove(comparedTid);
//                    newCandidate.removeAll(comparedTrack.candidateInfo);
//                    for (Integer tid : newCandidate) {
//                        Trajectory<Segment> comparededTrack = trackMap.get(tid);
//                        SimilarState stated = comparedTrack.getSimilarState(tid);
//                        if (stated == null)
////                            stated = Constants.getDTW(comparedTrack, comparededTrack);
//                            stated = Constants.getHausdorff(comparedTrack, comparededTrack);
//                        comparedTrack.relatedInfo.put(stated, stated);
//                        comparededTrack.relatedInfo.put(stated, stated);
//                        comparedTrack.candidateInfo.add(tid);
//                        comparedTrack.updateCandidateInfo(tid);
//                    }
//                    comparedTrack.threshold = comparedTrack.calculateThreshold();
//                    comparedPruning = comparedTrack.rect.clone().extendLength(comparedTrack.threshold - oldThreshold);
//                    pruneIndex.alterELem(comparedTrack, comparedPruning);
//                }
//            }
//        }
//        for (Integer compareTid : removeRI) {
//            if (!track.candidateInfo.contains(compareTid)) {
//                Trajectory<Segment> compareTrack = trackMap.get(compareTid);
//                SimilarState state = track.getSimilarState(compareTid);
//                track.relatedInfo.remove(state);
//                compareTrack.relatedInfo.remove(state);
//            }
//        }
//
//        //重新做一次裁剪和距离计算
//        track.sortCandidateInfo();
//        threshold = track.calculateThreshold();
//        MBR.getUnionRectangle(seg.rect);
//        Rectangle newPruning = MBR.clone().extendLength(threshold);
//        Set<Integer> newCandidate = pointIndex.getRegionTIDs(newPruning, true);
//        newCandidate.remove(track.TID);
//        newCandidate.removeAll(track.candidateInfo);
//        for (Integer tid : newCandidate) {
//            Trajectory<Segment> comparedTrack = trackMap.get(tid);
//            SimilarState state = track.getSimilarState(tid);
//            if (state == null) {
////                state = Constants.getDTW(track, comparedTrack);
//                state = Constants.getHausdorff(track, comparedTrack);
//                track.relatedInfo.put(state, state);
//                comparedTrack.relatedInfo.put(state, state);
//            }
//            track.candidateInfo.add(tid);
//            track.updateCandidateInfo(tid);
//        }
//
//        //计算新的裁剪区域，用裁剪裁剪区域计算候选轨迹集。除去老的候选轨迹集中
//        //不会再被用到的相似度计算中间状态
//        track.threshold = track.calculateThreshold();
//        newPruning = MBR.clone().extendLength(track.threshold);
//        newCandidate = pointIndex.getRegionTIDs(newPruning, true);
//        Set<Integer> oldCandidate = new HashSet<>(track.candidateInfo);
//        oldCandidate.removeAll(newCandidate);
//        track.candidateInfo.removeAll(oldCandidate);
//        for (Integer tid : oldCandidate) {
//            Trajectory<Segment> comparedTrack = trackMap.get(tid);
//            if (!comparedTrack.candidateInfo.contains(track.TID)) {
//                SimilarState state = track.getSimilarState(tid);
//                track.relatedInfo.remove(state);
//                comparedTrack.relatedInfo.remove(state);
//            }
//        }
//
//        //更新裁剪域索引信息
//        pruneIndex.alterELem(track, newPruning);
//    }
//
//    private void newTrackCalculate(Trajectory<Segment> track) throws CloneNotSupportedException {
//        Segment segment = track.elems.getFirst();
//
//        //筛选出计算阈值的轨迹集合，得出裁剪域
//        Set<Integer> selectedTIDs = new HashSet<>();  //阈值计算轨迹集
////        Rectangle searchRegion = (new Rectangle(new Point(trackPoint.data.clone()), new Point(trackPoint.data.clone()))).extendLength(30.0); //查询轨迹MBR
//        Rectangle searchRegion = segment.rect.clone().extendLength(30.0); //查询轨迹MBR
//        while (selectedTIDs.size() <= Constants.topK * Constants.KNum) { //阈值计算轨迹集的元素数要大于Constants.k*Constants.cDTW。
////            selectedTIDs = pointIndex.getRegionTIDs(searchRegion, true);
//            selectedTIDs = pointIndex.getRegionTIDs(searchRegion, false);
//            searchRegion.extendMultiple(0.5);   //查询轨迹MBR扩展
//        }
//        selectedTIDs.remove(track.TID);
//        Constants.cutTIDs(selectedTIDs);
//        for (Integer comparedTid : selectedTIDs) {
//            Trajectory<Segment> comparedTrack = trackMap.get(comparedTid);
////            SimilarState state = Constants.getDTW(track, comparedTrack);
//            SimilarState state = Constants.getHausdorff(track, comparedTrack);
//            track.relatedInfo.put(state, state);
//            track.candidateInfo.add(comparedTid);
//        }
//        track.sortCandidateInfo();
//        Rectangle pruneArea = track.getPruningRegion(track.calculateThreshold());
//
//        //用裁剪域筛选出候选轨迹集，计算距离并排序
////        Set<Integer> needCompareTids = pointIndex.getRegionTIDs(pruneArea, true);
//        Set<Integer> needCompareTids = pointIndex.getRegionTIDs(pruneArea, false);
//        needCompareTids.remove(track.TID);
//        List<SimilarState> result = new ArrayList<>();
//        for (Integer compareTid : needCompareTids) {
//            SimilarState state = track.getSimilarState(compareTid);
//            if (state == null)
////                state = Constants.getDTW(track, trackMap.get(compareTid));
//                state = Constants.getHausdorff(track, trackMap.get(compareTid));
//            result.add(state);
//        }
//        Collections.sort(result);
//
//        //紧缩裁剪区以查找需要保留相似度计算中间结果的轨迹
//        track.threshold = result.get(Constants.topK - 1).distance;
//        track.rect = track.getPruningRegion(track.threshold);
////        Set<Integer> needStoreTids = pointIndex.getRegionTIDs(track.rect, true);
//        Set<Integer> needStoreTids = pointIndex.getRegionTIDs(track.rect, false);
//        needCompareTids.removeAll(needStoreTids);
//        for (Integer compareTid : needCompareTids)
//            result.remove(new SimilarState(track.TID, compareTid, null, null));
//        List<Integer> list = new ArrayList<>();
//        track.relatedInfo.clear();
//        for (SimilarState state : result) {
//            list.add(state.comparedTID);
//            track.relatedInfo.put(state, state);
//            trackMap.get(state.comparedTID).relatedInfo.put(state, state);
//        }
//        track.candidateInfo = list;
//
//        //将裁剪区域插入索引中
//        pruneIndex.insert(track);
//    }
//
//    private void initCalculate() throws CloneNotSupportedException {
//        for (Trajectory<Segment> track : trackMap.values()) {
//            List<Trajectory<Segment>> list = new ArrayList<>(trackMap.values());
//            list.remove(track);
//            track.initCache(list);
//            track.threshold = track.calculateThreshold();
//            track.rect = track.getPruningRegion(track.threshold);
//            track.data = track.rect.getCenter().data;
//            pruneIndex.insert(track);
//        }
//    }
//
//    private void mayBeAnotherTopk(Segment seg, Trajectory<Segment> track) throws CloneNotSupportedException {
//        List<Trajectory<Segment>> list = pruneIndex.rectQuery(new Rectangle(seg.p2, seg.p2), false);
//        list.remove(track);
//        for (Trajectory<Segment> trajectory : list) {
//            if (!trajectory.candidateInfo.contains(track.TID)) {
//                double oldThreshold = trajectory.calculateThreshold();
//                SimilarState state = trajectory.getSimilarState(track.TID);
//                if (state == null) {
////                    state = Constants.getDTW(track, trajectory);
//                    state = Constants.getHausdorff(track, trajectory);
//                    track.relatedInfo.put(state, state);
//                    trajectory.relatedInfo.put(state, state);
//                }
//                trajectory.candidateInfo.add(track.TID);
//                trajectory.updateCandidateInfo(track.TID);
//                double newThreshold = trajectory.calculateThreshold();
//                if (!Constants.isEqual(oldThreshold, newThreshold)) {
//                    Rectangle comparedPruning = trajectory.rect.clone().extendLength(newThreshold - oldThreshold);
//                    if (oldThreshold - trajectory.threshold > 20.0) {
//                        cacheTighten(trajectory, comparedPruning, -1);
//                        trajectory.threshold = newThreshold;
//                    }
//                    pruneIndex.alterELem(trajectory, comparedPruning);
//                }
//            }
//        }
//    }
//
//    private boolean cacheTighten(Trajectory<Segment> trajectory, Rectangle comparedPruning, int noRemove) {
//        Set<Integer> newCandidate = pointIndex.getRegionTIDs(comparedPruning, true);
//        newCandidate.remove(trajectory.TID);
//        Set<Integer> oldCandidate = new HashSet<>(trajectory.candidateInfo);
//        for (Integer tid : newCandidate) {
//            if (!oldCandidate.contains(tid))
//                throw new IllegalArgumentException("error state");
//        }
//
//        oldCandidate.removeAll(newCandidate);
//        boolean res = oldCandidate.remove(noRemove);
//        if (res)
//            trajectory.candidateInfo.remove(Integer.valueOf(noRemove));
//        for (Integer tid : oldCandidate) {
//            trajectory.candidateInfo.remove(tid);
//            SimilarState state0 = trajectory.getSimilarState(tid);
//            Trajectory<Segment> comparedTrack = trackMap.get(tid);
//            if (!comparedTrack.candidateInfo.contains(trajectory.TID)) {
//                comparedTrack.relatedInfo.remove(state0);
//                trajectory.relatedInfo.remove(state0);
//            }
//        }
//        return res;
//    }
//
//    public boolean cacheCheck() throws CloneNotSupportedException {
//        if (pruneIndex.root.elemNum != trackMap.size())
//            return false;
//        pruneIndex.check();
//        pointIndex.check();
//        for (Trajectory<Segment> track : trackMap.values()) {
//            if (!trackCheck(track))
//                return false;
//        }
//        return true;
//    }
//
//    public boolean initCheck() throws CloneNotSupportedException {
//        for (Trajectory<Segment> track : trackMap.values()) {
//            if ( track.candidateInfo.size() != Constants.topK * Constants.KNum )
//                return false;
//            if (!trackCheck(track))
//                return false;
//        }
//        return true;
//    }
//
//    public boolean trackCheck(Trajectory<Segment> track) throws CloneNotSupportedException {
//        if(!track.leaf.elms.contains(track))
//            return false;
//        for (Segment trackPoint : track.elems) {
//            if (!trackPoint.leaf.elms.contains(trackPoint))
//                return false;
//        }
//        for (SimilarState state : track.relatedInfo.values()) {
//            int comparedTid;
//            if (state.comparedTID == track.TID)
//                comparedTid = state.comparingTID;
//            else
//                comparedTid = state.comparedTID;
//            Trajectory<Segment> comparedTrack = trackMap.get(comparedTid);
//            if (!comparedTrack.candidateInfo.contains(track.TID) && !track.candidateInfo.contains(comparedTid))
//                return false;
//        }
//        Rectangle rect = track.getPruningRegion(track.calculateThreshold());
//        if (!rect.equals(track.rect))
//            return false;
//        if (track.candidateInfo.size() < Constants.topK)
//            return false;
//        for (Integer tid : track.candidateInfo) {
//            if(track.getSimilarState(tid) == null)
//                return false;
//        }
//        for (int i = 0; i < track.candidateInfo.size()-1; i++) {
//            SimilarState state1 = track.getSimilarState(track.candidateInfo.get(i));
//            SimilarState state2 = track.getSimilarState(track.candidateInfo.get(i+1));
//            if (Double.compare(state1.distance,state2.distance) > 0)
//                return false;
//        }
//        Set<Integer> yz = pointIndex.getRegionTIDs(track.rect, true);
//        yz.remove(track.TID);
//        for (Integer tid : yz) {
//            if (!track.candidateInfo.contains(tid))
//                return false;
//        }
//        return true;
//    }

    @Override
    public void open(Configuration parameters)  {
//        pointIndex = new RCtree(8,1,17, Constants.globalRegion,0);
//        pointIndex.hasTIDs = true;
//        ((RCDataNode<Segment>)pointIndex.root).TIDs = new HashSet<>();
//        pruneIndex = new RCtree(8,1,17, Constants.globalRegion,0);
//        trackMap = new HashMap<>();
//        singlePointMap = new HashMap<>();
//        appendQueue = new LinkedList<>();
//        curAppend = new Tuple2<>(0L, new HashSet<>());
    }


}
