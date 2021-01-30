package com.ada.DTflinkFunction;

import com.ada.Hausdorff.Hausdorff;
import com.ada.Hausdorff.SimilarState;
import com.ada.QBSTree.Index;
import com.ada.common.collections.Judge;
import com.ada.geometry.track.TrackHauOne;
import com.ada.geometry.track.TrackKeyTID;
import com.ada.globalTree.GDataNode;
import com.ada.globalTree.GNode;
import com.ada.common.Constants;
import com.ada.geometry.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.roaringbitmap.RoaringBitmap;

import java.io.Serializable;
import java.util.*;

public class DTConstants implements Serializable {

    static <T extends TrackHauOne> Rectangle tightenThresholdCommon(T track,
                                                                    Map<Integer, T> trackMap) {
        double newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
        Rectangle pruneArea = track.rect.clone().extendLength(newThreshold - track.threshold);
        cacheTighten(track, trackMap, t -> !pruneArea.isInternal(t.rect.clone().extendLength(-t.threshold)));
        track.threshold = newThreshold;
        return pruneArea;
    }

    public static <T extends TrackHauOne> void supplyCandidate(T track,
                                                               Rectangle MBR,
                                                               Map<Integer, T> passTrackMap,
                                                               Map<Integer, T> topKTrackMap,
                                                               Index<Segment> segmentIndex,
                                                               boolean isPass){
        Integer TID = track.trajectory.TID;
        if (isPass) {
            List<SimilarState> list = new ArrayList<>(track.getRelatedInfo().keySet());
            if (topKTrackMap != null)
                list.removeIf(state -> topKTrackMap.containsKey(state.getStateAnoTID(TID)));
            java.util.Collections.sort(list);
            List<Integer> newCanDi = new ArrayList<>();
            for (int i = 0; i < Constants.topK + Constants.t && i < list.size(); i++) {
                newCanDi.add(list.get(i).getStateAnoTID(TID));
            }
            track.candidateInfo.removeAll(newCanDi);
            while (!track.candidateInfo.isEmpty()) track.removeICandidate(track.candidateInfo.size(), passTrackMap);
            track.candidateInfo = newCanDi;
        }
        if (track.candidateInfo.size() < Constants.topK + Constants.t){ //没有足够的候选轨迹
            Rectangle pruneArea;
            if (track.candidateInfo.size() == 0){
                pruneArea = MBR.clone().extendLength(Constants.extend);
            }else {
                pruneArea = MBR.clone().extendLength(track.getKCanDistance(Constants.topK + Constants.t).distance);
            }
            //筛选出计算阈值的轨迹集合，得出裁剪域
            Set<Integer> selectedTIDs = new HashSet<>();  //阈值计算轨迹集
            while (selectedTIDs.size() < Constants.topK + Constants.t) { //阈值计算轨迹集的元素数要大于Constants.k*Constants.cDTW。
                selectedTIDs = segmentIndex.getInternalTIDs(pruneArea);
                pruneArea.extendMultiple(0.3);   //查询轨迹MBR扩展
            }
            selectedTIDs.remove(TID);
            selectedTIDs.removeAll(track.candidateInfo);
            for (Integer selectedTID : selectedTIDs)
                track.addTrackCandidate(passTrackMap.get(selectedTID));
            track.sortCandidateInfo();
        }
    }

    /**
     * 轨迹trajectory的裁剪域变小了，删除一些缓存的中间结果
     */
    static <T extends TrackHauOne> void cacheTighten(T track,
                                                     Map<Integer, T> trackMap,
                                                     Judge<T> outPruneArea) {
        for (int i = Constants.topK + Constants.t; i < track.candidateInfo.size(); i++) {
            Integer comparedTID = track.candidateInfo.get(i);
            T comparedTrack = trackMap.get(comparedTID);
            if (outPruneArea.action(comparedTrack)){
                track.removeTIDCandidate(comparedTrack);
                i--;
            }
        }
    }

    static <T extends TrackHauOne> Rectangle newTrackCalculate(TrackHauOne track,
                                                               Rectangle MBR,
                                                               Rectangle pruneArea,
                                                               Index<Segment> segmentIndex,
                                                               Map<Integer, T> trackMap,
                                                               boolean hasComparedState,
                                                               boolean hasAlter) {
        Integer TID = track.trajectory.TID;
        //筛选出计算阈值的轨迹集合，得出裁剪域
        Set<Integer> selectedTIDs = new HashSet<>();  //阈值计算轨迹集
        while (selectedTIDs.size() < Constants.topK * Constants.KNum) { //阈值计算轨迹集的元素数要大于Constants.k*Constants.cDTW。
            selectedTIDs = segmentIndex.getInternalTIDs(pruneArea);
            pruneArea.extendMultiple(0.3);   //查询轨迹MBR扩展
        }
        selectedTIDs.remove(TID);
        Constants.cutTIDs(selectedTIDs);
        List<SimilarState> result = new ArrayList<>();
        if (hasComparedState){
            for (Integer comparedTid : selectedTIDs) {
                TrackHauOne comparedTrack = trackMap.get(comparedTid);
                SimilarState state = comparedTrack.getSimilarState(TID);
                if (state == null)
                    state = Hausdorff.getHausdorff(track.trajectory, comparedTrack.trajectory);
                result.add(state);
            }
        }else {
            for (Integer comparedTid : selectedTIDs) {
                TrackHauOne comparedTrack = trackMap.get(comparedTid);
                SimilarState state = Hausdorff.getHausdorff(track.trajectory, comparedTrack.trajectory);
                result.add(state);
            }
        }

        Collections.sort(result);
        SimilarState thresholdState = result.get(Constants.topK + Constants.t - 1);
        double threshold = thresholdState.distance;
        pruneArea = MBR.clone().extendLength(threshold);

        //用裁剪域筛选出候选轨迹集，计算距离并排序
        Set<Integer> needCompareTIDS = segmentIndex.getInternalNoIPTIDs(pruneArea);
        needCompareTIDS.remove(TID);
        List<SimilarState> needCompareState = new ArrayList<>();
        SimilarState tmpState = new SimilarState(TID, -1, null, null);
        if (hasComparedState){
            for (Integer compareTid : needCompareTIDS) {
                T comparedTrack = trackMap.get(compareTid);
                SimilarState state = comparedTrack.getSimilarState(TID);
                if (state == null){
                    tmpState.setComparedTID(compareTid);
                    int index = result.indexOf(tmpState);
                    if (index == -1){
                        tmpState.convert();
                        index = result.indexOf(tmpState);
                        if (index == -1)
                            state = Hausdorff.getHausdorff(track.trajectory, comparedTrack.trajectory);
                        else
                            state = result.get(index);
                    }else {
                        state = result.get(index);
                    }
                }
                needCompareState.add(state);
            }
        }else {
            for (Integer compareTid : needCompareTIDS) {
                tmpState.setComparedTID(compareTid);
                int index = result.indexOf(tmpState);
                SimilarState state;
                if (index == -1)
                    state = Hausdorff.getHausdorff(track.trajectory, trackMap.get(compareTid).trajectory);
                else
                    state = result.get(index);
                needCompareState.add(state);
            }
        }
        Collections.sort(needCompareState);

        //紧缩裁剪区以查找需要保留相似度计算中间结果的轨迹
        if (needCompareState.indexOf(thresholdState) > Constants.topK + Constants.t*2 - 1) {
            threshold = needCompareState.get(Constants.topK + Constants.t - 1).distance;
            pruneArea = MBR.clone().extendLength(threshold);
            for (int i = 0; i < Constants.topK + Constants.t; i++) {
                SimilarState state = needCompareState.get(i);
                Integer comparedTID = state.getStateAnoTID(TID);
                track.candidateInfo.add(comparedTID);
                state = trackMap.get(comparedTID).putRelatedInfo(state);
                track.putRelatedInfo(state);
            }
            Judge<T> inPruneArea;
            Rectangle finalPruneArea = pruneArea;
            if (hasAlter){
                inPruneArea = t -> !t.outSideRectangle(finalPruneArea);
            }else {
                inPruneArea = t -> finalPruneArea.isInternal(t.rect.clone().extendLength(-t.threshold));
            }
            for (int i = Constants.topK + Constants.t; i < needCompareState.size(); i++) {
                SimilarState state = needCompareState.get(i);
                Integer comparedTID = state.getStateAnoTID(TID);
                T comparedTrack = trackMap.get(comparedTID);
                if (inPruneArea.action(comparedTrack)){
                    track.candidateInfo.add(comparedTID);
                    state = trackMap.get(comparedTID).putRelatedInfo(state);
                    track.putRelatedInfo(state);
                }
            }
        } else {
            for (SimilarState state : needCompareState) {
                Integer comparedTID = state.getStateAnoTID(TID);
                track.candidateInfo.add(comparedTID);
                state = trackMap.get(comparedTID).putRelatedInfo(state);
                track.putRelatedInfo(state);
            }
        }
        track.threshold = threshold;
        return pruneArea;
    }



    /**
     * 轨迹track的裁剪域变大，topK结果变得不安全了，需要重新计算track的topK结果
     */
    static <T extends TrackHauOne> Rectangle enlargePrune(T track,
                                                          double newThreshold,
                                                          Index<Segment> segmentIndex,
                                                          Map<Integer, T> trackMap) {
        Rectangle pruneArea;
        pruneArea = track.rect.clone().extendLength(newThreshold - track.threshold);
        Set<Integer> newCandidate = segmentIndex.getInternalNoIPTIDs(pruneArea);
        newCandidate.remove(track.trajectory.TID);
        for (Integer id : track.candidateInfo) newCandidate.remove(id);
        track.trackAddCandidates(newCandidate, trackMap);
        //紧缩裁剪区以查找需要保留相似度计算中间结果的轨迹
        if (track.getKCanDistance(Constants.topK + Constants.t*2).distance < newThreshold) {
            newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
            pruneArea = track.rect.clone().extendLength(newThreshold - track.threshold);
            Rectangle finalPruneArea = pruneArea;
            cacheTighten(track, trackMap, t -> !finalPruneArea.isInternal(t.rect.clone().extendLength(-t.threshold)));
        }
        track.threshold = newThreshold;
        return pruneArea;
    }


    /**
     * 已有的轨迹有新的采样点上传或者有采样点过时，重新计算缓存中的相似度中间结果后调用该方法。
     * 进行再一次的topK结果计算。
     * @param MBR 轨迹的最小外包矩形
     */
    static <T extends TrackHauOne> Rectangle recalculateTrackTopK(T track,
                                                                  Rectangle MBR,
                                                                  Index<Segment> segmentIndex,
                                                                  Map<Integer, T> trackMap,
                                                                  boolean hasAlter) {
        //使用缓存的中间状态得出裁剪域添加新的topK候选轨迹
        Integer TID = track.trajectory.TID;
        double newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
        Rectangle pruneArea = MBR.clone().extendLength(newThreshold);
        Set<Integer> newCandidate = segmentIndex.getInternalNoIPTIDs(pruneArea);
        newCandidate.remove(TID);
        Set<Integer> oldCandidate = new HashSet<>(track.candidateInfo);
        oldCandidate.removeAll(newCandidate);
        newCandidate.removeAll(track.candidateInfo);
        for (Integer tid : oldCandidate)
            track.removeTIDCandidate(tid, trackMap);
        track.trackAddCandidates(newCandidate, trackMap);
        //计算新的裁剪区域，用裁剪裁剪区域计算候选轨迹集。除去老的候选轨迹集中
        //不会再被用到的相似度计算中间状态
        if (track.getKCanDistance(Constants.topK + Constants.t*2).distance < newThreshold){
            newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
            pruneArea = MBR.clone().extendLength(newThreshold);
            Judge<T> outPruneArea;
            Rectangle finalPruneArea = pruneArea;
            if (hasAlter){
                outPruneArea = t -> t.outSideRectangle(finalPruneArea);
            }else {
                outPruneArea = t -> !finalPruneArea.isInternal(t.rect.clone().extendLength(-t.threshold));
            }
            cacheTighten(track, trackMap, outPruneArea);
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
                                                       Set<Integer> emptyTIDs,
                                                       Index<Segment> segmentIndex,
                                                       Map<Integer, T> trackMap) {
        for (Integer tid : outTIDs) {
            T track = trackMap.get(tid);
            if (track != null) {
                List<Segment> timeElms = track.trajectory.removeElem(startWindow);
                if (timeElms.size() != 0) {
                    for (Segment segment : timeElms) segmentIndex.delete(segment);
                    if (track.trajectory.elms.size() == 0) emptyTIDs.add(tid);
                }
            }
        }
        for (Integer tid : inAndOutTIDs) {
            T track = trackMap.get(tid);
            if (track != null) {
                List<Segment> timeElms = track.trajectory.removeElem(startWindow);
                if (timeElms.size() != 0) {
                    for (Segment segment : timeElms) segmentIndex.delete(segment);
                }
            }
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
