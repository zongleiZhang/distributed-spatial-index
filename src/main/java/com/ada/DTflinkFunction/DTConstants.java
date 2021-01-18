package com.ada.DTflinkFunction;

import com.ada.Hausdorff.Hausdorff;
import com.ada.Hausdorff.SimilarState;
import com.ada.globalTree.GDataNode;
import com.ada.globalTree.GNode;
import com.ada.QBSTree.RCtree;
import com.ada.common.Constants;
import com.ada.geometry.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.roaringbitmap.RoaringBitmap;

import java.io.Serializable;
import java.util.*;

public class DTConstants implements Serializable {

    static <T extends TrackHauOne> Rectangle tightenThresholdCommon(T track,
                                                                    int notRemove,
                                                                    List<Integer> removeRI,
                                                                    Map<Integer, T> trackMap) {
        double newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
        Rectangle pruneArea = track.rect.clone().extendLength(newThreshold - track.threshold);
        if (DTConstants.cacheTighten(track, pruneArea, notRemove, trackMap))
            removeRI.add(track.trajectory.TID);
        track.threshold = newThreshold;
        return pruneArea;
    }

    /**
     * 轨迹trajectory的裁剪域变小了，删除一些缓存的中间结果
     * @param pruneArea 新的裁剪与大小
     * @return track作为noRemove轨迹的related track是否可以在relatedInfo中删除，true可以，false不可以
     */
    static <T extends TrackHauOne> boolean cacheTighten(T track,
                                                        Rectangle pruneArea,
                                                        Integer noRemove,
                                                        Map<Integer, T> trackMap) {
        boolean res = false;
        for (int i = Constants.topK + Constants.t; i < track.candidateInfo.size(); i++) {
            Integer comparedTID = track.candidateInfo.get(i);
            T comparedTrack = trackMap.get(comparedTID);
            if (comparedTrack.outSideRectangle(pruneArea)){
                if (noRemove.equals(comparedTID)){
                    res = true;
                    track.candidateInfo.remove(noRemove);
                    T noRemoveTrack = trackMap.get(noRemove);
                    if (noRemoveTrack.candidateInfo.contains(track.trajectory.TID))
                        res = false;
                    else
                        track.removeRelatedInfo(noRemove);
                }else {
                    track.removeTIDCandidate(comparedTrack);
                }
                i--;
            }
        }
        return res;
    }

    static <T extends TrackHauOne> Rectangle newTrackCalculate(TrackHauOne track,
                                                               Rectangle MBR,
                                                               Rectangle pruneArea,
                                                               RCtree<Segment> segmentIndex,
                                                               Map<Integer, T> trackMap) {
        Integer TID = track.trajectory.TID;
        //筛选出计算阈值的轨迹集合，得出裁剪域
        Set<Integer> selectedTIDs = new HashSet<>();  //阈值计算轨迹集
        while (selectedTIDs.size() < Constants.topK * Constants.KNum) { //阈值计算轨迹集的元素数要大于Constants.k*Constants.cDTW。
            selectedTIDs = segmentIndex.getIntersectTIDs(pruneArea);
            pruneArea.extendMultiple(0.3);   //查询轨迹MBR扩展
        }
        selectedTIDs.remove(TID);
        Constants.cutTIDs(selectedTIDs);
        List<SimilarState> result = new ArrayList<>();
        for (Integer comparedTid : selectedTIDs) {
            TrackHauOne comparedTrack = trackMap.get(comparedTid);
            SimilarState state = Hausdorff.getHausdorff(track.trajectory, comparedTrack.trajectory);
            result.add(state);
        }
        Collections.sort(result);
        SimilarState thresholdState = result.get(Constants.topK + Constants.t - 1);
        double threshold = thresholdState.distance;
        pruneArea = MBR.clone().extendLength(threshold);

        //用裁剪域筛选出候选轨迹集，计算距离并排序
        Set<Integer> needCompareTIDS = segmentIndex.getRegionInternalTIDs(pruneArea);
        needCompareTIDS.remove(TID);
        List<SimilarState> needCompareState = new ArrayList<>();
        for (Integer compareTid : needCompareTIDS) {
            int index = result.indexOf(new SimilarState(TID, compareTid, null, null));
            SimilarState state;
            if (index == -1)
                state = Hausdorff.getHausdorff(track.trajectory, trackMap.get(compareTid).trajectory);
            else
                state = result.get(index);
            needCompareState.add(state);
        }
        Collections.sort(needCompareState);

        //紧缩裁剪区以查找需要保留相似度计算中间结果的轨迹
        if (needCompareState.indexOf(thresholdState) > Constants.topK + Constants.t*2 - 1) {
            threshold = needCompareState.get(Constants.topK + Constants.t - 1).distance;
            pruneArea = MBR.clone().extendLength(threshold);
            for (int i = 0; i < Constants.topK + Constants.t; i++) {
                SimilarState state = needCompareState.get(i);
                track.candidateInfo.add(state.comparedTID);
                state = trackMap.get(state.comparedTID).putRelatedInfo(state);
                track.putRelatedInfo(state);
            }
            for (int i = Constants.topK + Constants.t; i < needCompareState.size(); i++) {
                SimilarState state = needCompareState.get(i);
                TrackHauOne comparedTrack = trackMap.get(state.comparedTID);
                if (!comparedTrack.outSideRectangle(pruneArea)){
                    track.candidateInfo.add(state.comparedTID);
                    state = trackMap.get(state.comparedTID).putRelatedInfo(state);
                    track.putRelatedInfo(state);
                }
            }
        } else {
            for (SimilarState state : needCompareState) {
                track.candidateInfo.add(state.comparedTID);
                state = trackMap.get(state.comparedTID).putRelatedInfo(state);
                track.putRelatedInfo(state);
            }
        }
        track.threshold = threshold;
        return pruneArea;
    }



    /**
     * 轨迹track的裁剪域变大，topK结果变得不安全了，需要重新计算track的topK结果
     */
    static <T extends TrackHauOne> Tuple2<Boolean, Rectangle> enlargePrune(T track,
                                                                          double newThreshold,
                                                                          int notRemove,
                                                                          RCtree<Segment> segmentIndex,
                                                                          Map<Integer, T> trackMap) {
        boolean res = false;
        Rectangle pruneArea;
        pruneArea = track.rect.clone().extendLength(newThreshold - track.threshold);
        Set<Integer> newCandidate = segmentIndex.getRegionInternalTIDs(pruneArea);
        newCandidate.remove(track.trajectory.TID);
        newCandidate.removeAll(track.candidateInfo);
        trackAddCandidate(track, newCandidate, trackMap);
        //紧缩裁剪区以查找需要保留相似度计算中间结果的轨迹
        if (track.getKCanDistance(Constants.topK + Constants.t*2).distance < newThreshold) {
            newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
            pruneArea = track.rect.clone().extendLength(newThreshold - track.threshold);
            res = cacheTighten(track, pruneArea, notRemove,trackMap);
        }
        track.threshold = newThreshold;
        return new Tuple2<>(res, pruneArea);
    }

    /**
     * 轨迹track添加一些候选轨迹newCandidate
     */
    private static <T extends TrackHauOne> void trackAddCandidate(T track,
                                                                  Set<Integer> newCandidate,
                                                                  Map<Integer, T> trackMap) {
        for (Integer comparedTid : newCandidate) {
            SimilarState state = track.getSimilarState(comparedTid);
            if (state == null) {
                TrackHauOne comparedTrack = trackMap.get(comparedTid);
                state = Hausdorff.getHausdorff(track.trajectory, comparedTrack.trajectory);
                state = comparedTrack.putRelatedInfo(state);
                track.putRelatedInfo(state);
            }
            track.candidateInfo.add(comparedTid);
            track.updateCandidateInfo(comparedTid);
        }
    }


    /**
     * 已有的轨迹有新的采样点上传或者有采样点过时，重新计算缓存中的相似度中间结果后调用该方法。
     * 进行再一次的topK结果计算。
     * @param MBR 轨迹的最小外包矩形
     */
    static <T extends TrackHauOne> Rectangle recalculateTrackTopK(T track,
                                                                  Rectangle MBR,
                                                                  RCtree<Segment> pointIndex,
                                                                  Map<Integer, T> trackMap) {
        //使用缓存的中间状态得出裁剪域添加新的topK候选轨迹
        Integer TID = track.trajectory.TID;
        double newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
        Rectangle pruneArea = MBR.clone().extendLength(newThreshold);
        Set<Integer> newCandidate = pointIndex.getRegionInternalTIDs(pruneArea);
        newCandidate.remove(TID);
        Set<Integer> oldCandidate = new HashSet<>(track.candidateInfo);
        oldCandidate.removeAll(newCandidate);
        newCandidate.removeAll(track.candidateInfo);
        for (Integer tid : oldCandidate)
            track.removeTIDCandidate(tid, trackMap);
        trackAddCandidate(track, newCandidate, trackMap);
        //计算新的裁剪区域，用裁剪裁剪区域计算候选轨迹集。除去老的候选轨迹集中
        //不会再被用到的相似度计算中间状态
        if (track.getKCanDistance(Constants.topK + Constants.t*2).distance < newThreshold){
            newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
            pruneArea = MBR.clone().extendLength(newThreshold);
            for (int i = Constants.topK + Constants.t; i < track.candidateInfo.size(); i++) {
                Integer comparedTID = track.candidateInfo.get(i);
                T comparedTrack = trackMap.get(comparedTID);
                if (comparedTrack.outSideRectangle(pruneArea)) {
                    track.removeTIDCandidate(comparedTrack);
                    i--;
                }
            }
        }
        track.threshold = newThreshold;
        //更新裁剪域索引信息
        return pruneArea;
    }


    /**
     * 删除轨迹集TIDs中每个条轨迹的早于startWindow的轨迹段，并将删除的采样点添加到removeElemMap中。
     */
    static <T extends TrackHauOne> void removeSegment( RoaringBitmap outTIDs,
                                                       RoaringBitmap inAndOutTIDs,
                                                       long startWindow,
                                                       Set<T> emptyTracks,
                                                       RCtree<Segment> pointIndex,
                                                       Map<Integer, T> trackMap) {
        for (Integer tid : outTIDs) {
            T track = trackMap.get(tid);
            if (track != null) {
                List<Segment> timeElms = track.trajectory.removeElem(startWindow);
                if (timeElms.size() != 0) {
                    for (Segment segment : timeElms) pointIndex.delete(segment);
                    if (track.trajectory.elms.size() == 0) emptyTracks.add(track);
                }
            }
        }
        for (Integer tid : inAndOutTIDs) {
            T track = trackMap.get(tid);
            if (track != null) {
                List<Segment> timeElms = track.trajectory.removeElem(startWindow);
                if (timeElms.size() != 0) {
                    for (Segment segment : timeElms) pointIndex.delete(segment);
                }
            }
        }
    }


    /**
     * 轨迹集emptyElemMap的所有采样点都滑出窗口，删除相关数据。
     * @param pruneChangeTracks 记录由于删除track而导致其裁剪域发生变化的轨迹。不包括在hasSlideTrackIds中的轨迹
     */
    static <T extends TrackHauOne> void dealAllSlideOutTracks(T track,
                                                              RoaringBitmap inTIDs,
                                                              RoaringBitmap outTIDs,
                                                              RoaringBitmap inAndOutTIDs,
                                                              Set<T> pruneChangeTracks,
                                                              Set<T> emptyTracks,
                                                              Set<T> canSmallTracks,
                                                              Map<Integer, T> passTrackMap,
                                                              Map<Integer, T> topKTrackMap,
                                                              RCtree<T> pruneIndex) {
        Integer tid = track.trajectory.TID;
        pruneIndex.delete(track);
        for (SimilarState state : track.getRelatedInfo().values()) {
            int comparedTid = Constants.getStateAnoTID(state, tid);
            T comparedTrack = passTrackMap.get(comparedTid);
            if (comparedTrack == null) comparedTrack = topKTrackMap.get(comparedTid);
            if (emptyTracks.contains(comparedTrack))
                continue;
            int index = comparedTrack.candidateInfo.indexOf(tid);
            comparedTrack.candidateInfo.remove(tid);
            comparedTrack.removeRelatedInfo(state);
            if (index != -1) {
                if (!inAndOutTIDs.contains(comparedTid) && !outTIDs.contains(comparedTid) && !inTIDs.contains(comparedTid)){
                    pruneChangeTracks.add(comparedTrack);
                    //comparedTrack的候选轨迹集太少了
                    if (comparedTrack.candidateInfo.size() < Constants.topK) canSmallTracks.add(comparedTrack);
                }
            }
        }
    }

    static <T> void addOnePointQueue(LinkedList<T> queue,
                                    T t,
                                    Comparator<T> comparator){
        int index = 0;
        for (T q : queue) {
            if ( comparator.compare(q,t) <= 0 )
                index++;
            else
                break;
        }
        queue.add(index, t);
    }

    static void initCalculate(List<TrackHauOne> tracks0,
                               List<TrackHauOne> tracks2,
                               RCtree<TrackHauOne> pruneIndex) {
        for (TrackHauOne track : tracks0) {
            List<TrackHauOne> list = new ArrayList<>(tracks2);
            list.remove(track);
            for (TrackHauOne comparedTrack : list)
                Constants.addTrackCandidate(track, comparedTrack);
            track.sortCandidateInfo();
            track.threshold = track.getKCanDistance(Constants.t + Constants.topK).distance;
            track.rect = Constants.getPruningRegion(track.trajectory, track.threshold);
            track.data = track.rect.getCenter().data;
            pruneIndex.insert(track);
        }
    }

    /**
     * 轨迹track可能成为其它轨迹的topK结果
     */
    static void mayBeAnotherTopK(TrackHauOne track,
                                 RCtree<TrackHauOne> pruneIndex,
                                 Map<Integer, TrackHauOne> passTrackMap,
                                 Map<Integer, TrackHauOne> topKTrackMap) {
        Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
        List<Integer> list = pruneIndex.trackInternal(MBR);
        Integer TID = track.trajectory.TID;
        list.remove(TID);
        for (Integer comparedTid : list) {
            TrackHauOne comparedTrack = passTrackMap.get(comparedTid);
            if (comparedTrack == null)
                comparedTrack = topKTrackMap.get(comparedTid);
            if (!comparedTrack.candidateInfo.contains(TID)){
                Constants.addTrackCandidate(comparedTrack, track);
                comparedTrack.updateCandidateInfo(TID);
                if (comparedTrack.getKCanDistance(Constants.topK + Constants.t*2).distance < comparedTrack.threshold) {
                    double newThreshold = comparedTrack.getKCanDistance(Constants.topK + Constants.t).distance;
                    Rectangle pruneArea = comparedTrack.rect.clone().extendLength(newThreshold - comparedTrack.threshold);
                    DTConstants.cacheTighten(comparedTrack, pruneArea, -1, passTrackMap);
                    comparedTrack.threshold = newThreshold;
                    pruneIndex.alterELem(comparedTrack, pruneArea);
                }
            }
        }
    }

    /**
     * 轨迹的阈值可能发生变化时，调用该函数，进行修改阈值的一系列操作
     * @param notRemove 修改阈值时，可能删除缓存中的某个中间状态。但notRemoveTid状态不能现在删除，将其添加到removeRI，后续再删除
     */
    static <T extends RCtree<Segment>> void changeThreshold(TrackHauOne track,
                                                            int notRemove,
                                                            List<Integer> removeRI,
                                                            RCtree<TrackHauOne> pruneIndex,
                                                            T pointIndex,
                                                            Map<Integer, TrackHauOne> trackMap) {
        double dis = track.getKCanDistance(Constants.topK).distance;
        if (dis > track.threshold){
            //裁剪域变大，topK结果变得不安全了，需要重新计算comparedTrack的topK结果。
            dis = track.getKCanDistance(Constants.topK + Constants.t).distance;
            Tuple2<Boolean, Rectangle> tuple2 = DTConstants.enlargePrune(track, dis, notRemove, pointIndex, trackMap);
            if ( tuple2.f0 )
                removeRI.add(track.trajectory.TID);
            pruneIndex.alterELem(track, tuple2.f1);
            return;
        }
        dis = track.getKCanDistance(Constants.topK + Constants.t*2).distance;
        if (dis < track.threshold){
            //有更近的topK结果更新裁剪区域即可,为了避免频繁更新，要求threshold的变动超过20
            Rectangle pruneArea = DTConstants.tightenThresholdCommon(track,notRemove,removeRI,trackMap);
            pruneIndex.alterELem(track, pruneArea);
        }
    }



    static <T extends RCtree<Segment>> void dealCandidateSmall(Map<Integer, List<Segment>> removeElemMap,
                                                               Integer TID,
                                                               TrackHauOne track,
                                                               Map<Integer,TrackHauOne> passTrackMap,
                                                               Map<Integer,TrackHauOne> topKTrackMap,
                                                               RCtree<TrackHauOne> pruneIndex,
                                                               T pointIndex) {
        try {
            if (track == null) {
                track = passTrackMap.get(TID);
                if (track == null)
                    topKTrackMap.get(TID);
            }
            assert track != null;
            List<SimilarState> list = new ArrayList<>(track.getRelatedInfo().keySet());
            Collections.sort(list);
            List<Integer> newCanDi = new ArrayList<>();
            int i = 0;
            while (i < Constants.topK * Constants.KNum && i < list.size()) {
                SimilarState s = list.get(i);
                int id = Constants.getStateAnoTID(s, TID);
                newCanDi.add(id);
                i++;
            }
            if (newCanDi.size() < Constants.topK) {
                TrackHauOne tmpTrack = new TrackHauOne(null, null, null, track.trajectory.elms, TID, new ArrayList<>(), new HashMap<>());
                double threshold = track.getKCanDistance(Constants.topK).distance;
                Rectangle MBR = Constants.getPruningRegion(track.trajectory, 0.0);
                Rectangle pruneArea = MBR.clone().extendLength(threshold);
                tmpTrack.rect = DTConstants.newTrackCalculate(tmpTrack, MBR, pruneArea, pointIndex, passTrackMap);
                Set<SimilarState> stateSet = tmpTrack.getRelatedInfo().keySet();
                for (SimilarState state : track.getRelatedInfo().values()) {
                    if (!stateSet.contains(state)) {
                        int comparedTID = Constants.getStateAnoTID(state, TID);
                        TrackHauOne comparedTrack = passTrackMap.get(comparedTID);
                        if (comparedTrack == null)
                            comparedTrack = topKTrackMap.get(comparedTID);
                        if (comparedTrack.candidateInfo.contains(TID))
                            tmpTrack.putRelatedInfo(state);
                        else
                            comparedTrack.removeRelatedInfo(state);
                    }
                }
                track.setRelatedInfo(tmpTrack.getRelatedInfo());
                track.candidateInfo = tmpTrack.candidateInfo;
                track.threshold = tmpTrack.threshold;
                pruneIndex.alterELem(track, tmpTrack.rect);
            } else {
                track.candidateInfo.removeAll(newCanDi);
                while (!track.candidateInfo.isEmpty())
                    track.removeICandidate(1, passTrackMap);
                track.candidateInfo = newCanDi;
                if (removeElemMap.get(TID) != null) {
                    Rectangle MBR = Constants.getPruningRegion(track.trajectory, 0.0);
                    Rectangle pruneArea = DTConstants.recalculateTrackTopK(track, MBR, pointIndex, passTrackMap);
                    pruneIndex.alterELem(track, pruneArea);
                } else {
                    DTConstants.changeThreshold(track, -1, null, pruneIndex, pointIndex, passTrackMap);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static void addTrackTopK(TrackKeyTID track, Rectangle MBR, List<GDataNode> changeTopKLeaf) {
        for (GDataNode topKLeaf : changeTopKLeaf) {
            double bound = Constants.countEnlargeBound(MBR, topKLeaf.region);
            track.topKP.add(new GLeafAndBound(topKLeaf, bound) );
        }
    }


    static Tuple2<GNode, Double> countEnlarge(Collection<GDataNode> newLeafsSet, Rectangle MBR) {
        Tuple2<GNode, Double> res = new Tuple2<>(null, Double.MAX_VALUE);
        for (GDataNode dataNode : newLeafsSet) {
            double bound = Constants.countEnlargeBound(MBR, dataNode.region);
            if (res.f1 > bound){
                res.f0 = dataNode;
                res.f1 = bound;
            }
        }
        return res;
    }
}
