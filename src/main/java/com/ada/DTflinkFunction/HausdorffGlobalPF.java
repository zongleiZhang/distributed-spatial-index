package com.ada.DTflinkFunction;

import com.ada.QBSTree.RCtree;
import com.ada.common.ArrayQueue;
import com.ada.common.Arrays;
import com.ada.common.Constants;
import com.ada.common.Hausdorff;
import com.ada.geometry.*;
import com.ada.globalTree.GDataNode;
import com.ada.globalTree.GNode;
import com.ada.globalTree.GTree;
import com.ada.model.densityToGlobal.Density;
import com.ada.model.densityToGlobal.Density2GlobalElem;
import com.ada.model.globalToLocal.Global2LocalElem;
import com.ada.model.globalToLocal.Global2LocalPoints;
import com.ada.model.globalToLocal.Global2LocalTID;
import com.ada.common.collections.Collections;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;


public class HausdorffGlobalPF extends ProcessWindowFunction<Density2GlobalElem, Global2LocalElem, Integer, TimeWindow> {

    private boolean hasInit;
    private int subTask;
    private GTree globalTree;
    private Queue<int[][]> densityQue;
    private Queue<RoaringBitmap> tIDsQue;

    private Map<Integer, TrackKeyTID> trackMap;
    private Map<Integer, TrackPoint> singlePointMap;
    private RCtree<Segment> segmentIndex;
    private RCtree<TrackKeyTID> pruneIndex;
    private Collector<Global2LocalElem> out;
    private int count;

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<Density2GlobalElem> elements,
                        Collector<Global2LocalElem> out) {
        this.out = out;
        List<TrackKeyTID> newTracks = new ArrayList<>();
        Map<Integer, List<TrackPoint>> inPointsMap = new HashMap<>();
        int[][] density = preElements(elements, newTracks, inPointsMap);
        RoaringBitmap inputIDs = new RoaringBitmap();
        inputIDs.add(inPointsMap.keySet().stream().mapToInt(Integer::valueOf).toArray());
        tIDsQue.add(inputIDs);
        if (density != null){
            densityQue.add(density);
            Arrays.addArrsToArrs(globalTree.density, density, true);
        }

        long logicWinStart = context.window().getEnd() - Constants.windowSize*Constants.logicWindow;
        if (hasInit){
            RoaringBitmap outTIDs = new RoaringBitmap();
            if (densityQue.size() > Constants.logicWindow)  outTIDs = tIDsQue.remove();
            RoaringBitmap inAndOutTIDs = inputIDs.clone();
            inAndOutTIDs.and(outTIDs);
            RoaringBitmap inTIDs = inputIDs.clone();
            inTIDs.andNot(inAndOutTIDs);
            outTIDs.andNot(inAndOutTIDs);
            Set<TrackKeyTID> emptyTracks = new HashSet<>();
            DTConstants.removeSegment(outTIDs, inAndOutTIDs, logicWinStart, emptyTracks, segmentIndex, trackMap);
            //记录无采样点滑出，但其topK结果可能发生变化的轨迹
            Set<TrackKeyTID> pruneChangeTracks = new HashSet<>();
            //记录无采样点滑出，别的轨迹滑出导致其候选轨迹集小于K的轨迹
            Set<TrackKeyTID> canSmallTracks = new HashSet<>();
            //处理整条轨迹滑出窗口的轨迹
            for (TrackKeyTID track : emptyTracks) {
                Integer tid = track.trajectory.TID;
                track.enlargeTuple.f0.bitmap.remove(tid);
                for (GDataNode leaf : track.passP) leaf.bitmap.remove(tid);
                if (!track.topKP.isEmpty()){
                    for (GLeafAndBound gb : track.topKP.getList()) gb.leaf.bitmap.remove(tid);
                }
                DTConstants.dealAllSlideOutTracks(track, inTIDs, outTIDs, inAndOutTIDs, pruneChangeTracks, emptyTracks, canSmallTracks, trackMap,null, pruneIndex);
            }
            //处理整条轨迹未完全滑出窗口的轨迹
            processUpdatedTrack(inTIDs, outTIDs, inAndOutTIDs, inPointsMap, pruneChangeTracks, canSmallTracks);
            pruneChangeTracks.removeAll(canSmallTracks);
            for (TrackKeyTID track : pruneChangeTracks) {
                changeThreshold(track);
            }
            for (TrackKeyTID track : canSmallTracks) {
                dealCandidateSmall(track);
            }
            for (Integer tid : outTIDs) mayBeAnotherTopK(trackMap.get(tid));
            for (TrackKeyTID track : newTracks) mayBeAnotherTopK(track);
        }else { //轨迹足够多时，才开始计算，计算之间进行初始化
            if (density != null)
                globalTree.updateTree();
            if (tIDsQue.size() > Constants.logicWindow){ //窗口完整后才能进行初始化计算
                for (Integer tid : tIDsQue.remove())
                    trackMap.get(tid).trajectory.removeElem(logicWinStart);
                // globalTree中的每个叶节点中的轨迹数量超过Constants.topK才能进行初始化计算
                boolean canInit = true;
                for (GDataNode leaf : globalTree.getAllLeafs()) {
                    if (leaf.bitmap.toArray().length <= Constants.topK){
                        canInit = false;
                        break;
                    }
                }
                if (canInit){
                    hasInit = true;
                    initCalculate();
                }
            }
        }


        // 调整Global Index, 然后将调整结果同步到相关的Local Index中。
//        if (density != null){
//            densityQue.add(density);
//            Arrays.addArrsToArrs(globalTree.density, density, true);
//            if (densityQue.size() > Constants.logicWindow / Constants.densityFre) {
//                int[][] removed = densityQue.remove();
//                Arrays.addArrsToArrs(globalTree.density, removed, false);
//            }
//
//            //调整Global Index
//            Map<GNode, GNode> nodeMap = globalTree.updateTree();
//
//            //Global Index发生了调整，通知Local Index迁移数据，重建索引。
//            if (!nodeMap.isEmpty() && subTask == 0)
//                adjustLocalTasksRegion(nodeMap);
//        }
    }

    private void dealCandidateSmall(TrackKeyTID track) {
        Integer TID = track.trajectory.TID;
        if (track.enlargeTuple.f0 != track.passP.get(0)) track.enlargeTuple.f0.bitmap.remove(TID);
        List<SimilarState> list = new ArrayList<>(track.getRelatedInfo().keySet());
        java.util.Collections.sort(list);
        List<Integer> newCanDi = new ArrayList<>();
        for (int i = 0; i < Constants.topK + Constants.t && i < list.size(); i++) {
            newCanDi.add(Constants.getStateAnoTID(list.get(i), TID));
        }
        track.candidateInfo.removeAll(newCanDi);
        while (!track.candidateInfo.isEmpty()) track.removeICandidate(1, trackMap);
        track.candidateInfo = newCanDi;

        if (newCanDi.size() < Constants.topK + Constants.t) {
            Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
            TrackKeyTID tmpTrack = new TrackKeyTID(null, null, null, track.trajectory.elms, TID, new ArrayList<>(), new HashMap<>());
            Rectangle pruneArea = MBR.clone().extendLength(track.getKCanDistance(Constants.topK).distance);
            tmpTrack.rect = DTConstants.newTrackCalculate(tmpTrack, MBR, pruneArea, segmentIndex, trackMap);
            for (SimilarState state : track.getRelatedInfo().values()) {
                if (tmpTrack.getRelatedInfo().get(state) == null) {
                    TrackKeyTID comparedTrack = trackMap.get(Constants.getStateAnoTID(state, TID));
                    if (comparedTrack.candidateInfo.contains(TID)) {
                        tmpTrack.putRelatedInfo(state);
                    }else {
                        comparedTrack.removeRelatedInfo(state);
                    }
                }
            }
            track.setRelatedInfo(tmpTrack.getRelatedInfo());
            track.candidateInfo = tmpTrack.candidateInfo;
            track.threshold = tmpTrack.threshold;

            List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(tmpTrack.rect);
            if (track.topKP.isEmpty()){ //无topK扩展节点
                track.rect = tmpTrack.rect;
                track.data = tmpTrack.rect.getCenter().data;
                if (pruneAreaLeafs.size() == track.passP.size()){ //还无topK扩展节点
                    track.cutOffCandidate(trackMap);
                }else { //有topK扩展节点了
                    pruneIndex.insert(track);
                    globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                    outAddTopKTrackGB(track.topKP.getList(), Global2LocalPoints.ToG2LPoints(track.trajectory));
                }
            }else { //有topK扩展节点
                if (pruneAreaLeafs.size() == track.passP.size()){ //无topK扩展节点
                    track.cutOffCandidate(trackMap);
                    outDeleteTopKPTrackGB(TID, track.topKP.getList());
                    pruneIndexRemove(track);
                    track.rect = tmpTrack.rect;
                    track.data = tmpTrack.rect.getCenter().data;
                }else { //有topK扩展节点
                    pruneIndex.alterELem(track, tmpTrack.rect);
                    Set<GDataNode> newTopKLeafs = new HashSet<>(pruneAreaLeafs);
                    newTopKLeafs.removeAll(track.passP);
                    GLeafAndBound gb = new GLeafAndBound();
                    List<GLeafAndBound> deleteTopKGBs = track.topKP.getList();
                    List<GLeafAndBound> overlapTopKGBs = new ArrayList<>();
                    Global2LocalPoints trackPs = Global2LocalPoints.ToG2LPoints(track.trajectory);
                    for (Iterator<GDataNode> ite = newTopKLeafs.iterator(); ite.hasNext();) {
                        GDataNode leaf = ite.next();
                        gb.leaf = leaf;
                        int index = deleteTopKGBs.indexOf(gb);
                        if (index != -1) { //oldTopKs中有leaf
                            overlapTopKGBs.add(deleteTopKGBs.remove(index));
                            ite.remove();
                        }else { //oldTopKs中没有leaf
                            out.collect(new Global2LocalElem(leaf.leafID, (byte) 3, trackPs));
                            leaf.bitmap.add(TID);
                        }
                    }
                    outDeleteTopKPTrackGB(TID, deleteTopKGBs);
                    if (track.threshold > track.enlargeTuple.f1 ) globalTree.countEnlargeBound(track, pruneAreaLeafs, MBR);
                    for (GDataNode leaf : newTopKLeafs) overlapTopKGBs.add(new GLeafAndBound(leaf, Constants.countEnlargeBound(MBR, leaf.region)));
                    track.topKP.setList(overlapTopKGBs);
                }
            }
        }else {
            changeThreshold(track);
        }
        track.enlargeTuple.f0.bitmap.add(TID);
    }

    /**
     * 轨迹track有采样点滑出窗口，但不是全部。修改相关数据。
     */
    private void processUpdatedTrack(RoaringBitmap inTIDs,
                                     RoaringBitmap outTIDs,
                                     RoaringBitmap inAndOutTIDs,
                                     Map<Integer, List<TrackPoint>> inPointsMap,
                                     Set<TrackKeyTID> pruneChangeTracks,
                                     Set<TrackKeyTID> canSmallTracks) {
        RoaringBitmap calculatedTIDs = new RoaringBitmap();
        for (Integer tid : outTIDs) {
            TrackKeyTID track = trackMap.get(tid);
            for (SimilarState state : track.getRelatedInfo().values()) {
                int comparedTid = Constants.getStateAnoTID(state, tid);
                if (!calculatedTIDs.contains(comparedTid)) { //track与comparedTid的距离没有计算过
                    TrackKeyTID comparedTrack = trackMap.get(comparedTid);
                    if (inAndOutTIDs.contains(comparedTid)) {
                        Hausdorff.NOIOHausdorff(track.trajectory, comparedTrack.trajectory, inPointsMap.get(comparedTid), state);
                    }else if (outTIDs.contains(comparedTid)){
                        Hausdorff.NONOHausdorff(track.trajectory, comparedTrack.trajectory, state);
                    }else if (inTIDs.contains(comparedTid)){
                        Hausdorff.NOINHausdorff(track.trajectory, comparedTrack.trajectory, inPointsMap.get(comparedTid), state);
                    }else {
                        Hausdorff.NONNHausdorff(track.trajectory, comparedTrack.trajectory, state);
                        int oldIndex = comparedTrack.candidateInfo.indexOf(tid);
                        if (oldIndex != -1) {
                            comparedTrack.updateCandidateInfo(tid);
                            pruneChangeTracks.add(comparedTrack);
                        }
                    }
                }
            }
            track.sortCandidateInfo();
            recalculateOutPointTrack(track);
            calculatedTIDs.add(tid);
        }

        for (Integer tid : inTIDs) {
            TrackKeyTID track = trackMap.get(tid);
            List<TrackPoint> inPoints = inPointsMap.get(tid);
            Rectangle pointsMBR = Rectangle.pointsMBR(inPoints.toArray(new Point[0]));
            if (track.trajectory.elms.size() == inPoints.size() - 1){
                Rectangle pruneArea = pointsMBR.clone().extendLength(Constants.extend);
                track.rect = DTConstants.newTrackCalculate(track, pointsMBR, pruneArea, segmentIndex, trackMap);
                globalTree.countPartitions(pointsMBR, track);
                track.enlargeTuple.f0.bitmap.add(tid);
                Global2LocalPoints trackPs = Global2LocalPoints.ToG2LPoints(track.trajectory);
                if (!track.topKP.isEmpty()) {
                    outAddTopKTrackGB(track.topKP.getList(), trackPs);
                    pruneIndex.insert(track);
                }
                outAddPassTrack(track.passP, trackPs);
            }else {
                pointsMBR.getUnionRectangle(track.trajectory.elms.getLast());
                for (SimilarState state : track.getRelatedInfo().values()) {
                    int comparedTid = Constants.getStateAnoTID(state, tid);
                    if (!calculatedTIDs.contains(comparedTid)) { //track与comparedTid的距离没有计算过
                        TrackKeyTID comparedTrack = trackMap.get(comparedTid);
                        if (inAndOutTIDs.contains(comparedTid)) {
                            Hausdorff.INIOHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, inPointsMap.get(comparedTid), state);
                        } else if (outTIDs.contains(comparedTid)) {
                            Hausdorff.INNOHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, state);
                        } else if (inTIDs.contains(comparedTid)) {
                            Hausdorff.ININHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, inPointsMap.get(comparedTid), state);
                        } else {
                            Hausdorff.INNNHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, state);
                            addPruneChangeAndCanSmall(tid, comparedTrack, pointsMBR, pruneChangeTracks, canSmallTracks);
                        }
                    }
                }
                track.sortCandidateInfo();
                recalculateInPointTrack(track, inPoints, pointsMBR);
                calculatedTIDs.add(tid);
            }
        }

        for (Integer tid : inAndOutTIDs) {
            TrackKeyTID track = trackMap.get(tid);
            List<TrackPoint> inPoints = inPointsMap.get(tid);
            Rectangle pointsMBR = Rectangle.pointsMBR(inPoints.toArray(new Point[0]));
            for (SimilarState state : track.getRelatedInfo().values()) {
                int comparedTid = Constants.getStateAnoTID(state, tid);
                if (!calculatedTIDs.contains(comparedTid)) { //track与comparedTid的距离没有计算过
                    TrackKeyTID comparedTrack = trackMap.get(comparedTid);
                    if (inAndOutTIDs.contains(comparedTid)) {
                        Hausdorff.IOIOHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, inPointsMap.get(comparedTid), state);
                    }else if (outTIDs.contains(comparedTid)){
                        Hausdorff.IONOHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, state);
                    }else if (inTIDs.contains(comparedTid)){
                        Hausdorff.IOINHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, inPointsMap.get(comparedTid), state);
                    }else {
                        Hausdorff.IONNHausdorff(track.trajectory, inPoints, comparedTrack.trajectory, state);
                        addPruneChangeAndCanSmall(tid, comparedTrack, pointsMBR, pruneChangeTracks, canSmallTracks);
                    }
                }
            }
            track.sortCandidateInfo();
            recalculateInAndOutPointTrack(track, inPoints, pointsMBR);
            calculatedTIDs.add(tid);
        }
    }

    private void addPruneChangeAndCanSmall(Integer tid,
                                           TrackKeyTID comparedTrack,
                                           Rectangle pointsMBR,
                                           Set<TrackKeyTID> pruneChangeTracks,
                                           Set<TrackKeyTID> canSmallTracks) {
        int oldIndex = comparedTrack.candidateInfo.indexOf(tid);
        if (oldIndex != -1) {
            if (comparedTrack.rect.isInternal(pointsMBR)){
                comparedTrack.updateCandidateInfo(tid);
                pruneChangeTracks.add(comparedTrack);
            }else {
                comparedTrack.candidateInfo.remove(oldIndex);
                if (comparedTrack.candidateInfo.size() < Constants.topK){
                    canSmallTracks.add(comparedTrack);
                }else {
                    pruneChangeTracks.add(comparedTrack);
                }
            }
        }
    }

    private void recalculateInAndOutPointTrack(TrackKeyTID track,
                                               List<TrackPoint> points,
                                               Rectangle pointsMBR){
        try {
            Integer TID = track.trajectory.TID;
            if (track.enlargeTuple.f0 != track.passP.get(0)) track.enlargeTuple.f0.bitmap.remove(TID);
            Rectangle MBR = Constants.getPruningRegion(track.trajectory, 0.0);
            double newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
            Rectangle pruneArea = MBR.clone().extendLength(newThreshold);
            //往track经过的节点发送数据
            Global2LocalPoints trackPs = Global2LocalPoints.ToG2LPoints(track.trajectory);
            Global2LocalPoints addPs = new Global2LocalPoints(points);
            List<GDataNode> newMBRLeafs = globalTree.getIntersectLeafNodes(pointsMBR);
            List<GDataNode> deleteLeafs = (List<GDataNode>) Collections.removeAndGatherElms(track.passP, leaf -> !leaf.region.isIntersection(MBR));
            newMBRLeafs.removeIf(leaf -> track.passP.contains(leaf));
            if (!newMBRLeafs.isEmpty()) {
                if (track.topKP.isEmpty()){
                    for (GDataNode leaf : newMBRLeafs) {
                        out.collect(new Global2LocalElem(leaf.leafID, (byte) 2, trackPs));
                        leaf.bitmap.add(TID);
                    }
                }else {
                    List<GLeafAndBound> topKLeafs = track.topKP.getList();
                    for (GDataNode leaf : newMBRLeafs) {
                        GLeafAndBound gb = new GLeafAndBound(leaf, 0.0);
                        if (topKLeafs.contains(gb)) {
                            out.collect(new Global2LocalElem(leaf.leafID, (byte) 1, addPs));
                            out.collect(new Global2LocalElem(leaf.leafID, (byte) 7, new Global2LocalTID(TID)));
                            topKLeafs.remove(gb);
                        } else {
                            out.collect(new Global2LocalElem(leaf.leafID, (byte) 2, trackPs));
                            leaf.bitmap.add(TID);
                        }
                    }
                }
                track.passP.addAll(newMBRLeafs);
            }
            if (track.topKP.isEmpty()) { //没有topK扩展节点
                List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
                if (pruneAreaLeafs.size() == track.passP.size()){ //还没有topK扩展节点
                    globalTree.countEnlargeBound(track, track.passP, MBR);
                    track.rect = pruneArea;
                    track.data = pruneArea.getCenter().data;
                    track.threshold = newThreshold;
                }else { //有topK扩展节点了
                    pruneArea = extendTopKP(track, MBR);
                    Rectangle tmpArea = pruneArea;
                    pruneAreaLeafs.removeIf(leaf -> !leaf.region.isIntersection(tmpArea));
                    track.rect = pruneArea;
                    track.data = pruneArea.getCenter().data;
                    if (track.passP.size() == pruneAreaLeafs.size()) { //还没有topK扩展节点
                        track.cutOffCandidate(trackMap);
                        globalTree.countEnlargeBound(track, track.passP, MBR);
                    }else { //有topK扩展节点了
                        pruneIndex.insert(track);
                        globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                        for (GLeafAndBound gb : track.topKP.getList()) {
                            if (deleteLeafs.remove(gb.leaf)){
                                out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 6, new Global2LocalTID(TID)));
                            }else {
                                out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 3, trackPs));
                                gb.leaf.bitmap.add(TID);
                            }
                        }
                    }
                }
            }else {
                pruneArea = DTConstants.recalculateTrackTopK(track, MBR, segmentIndex, trackMap);
                List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
                if (pruneAreaLeafs.size() == track.passP.size()){ //没有topK扩展节点
                    track.cutOffCandidate(trackMap);
                    outDeleteTopKPTrackGB(TID, track.topKP.getList());
                    globalTree.countEnlargeBound(track, track.passP, MBR);
                    pruneIndex.delete(track);
                    track.topKP.clear();
                    track.rect = pruneArea;
                    track.data = pruneArea.getCenter().data;
                }else { //有topK扩展节点
                    pruneIndex.alterELem(track, pruneArea);
                    List<GLeafAndBound> oldTopK = track.topKP.getList();
                    globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                    for (GLeafAndBound gb : track.topKP.getList()){
                        if (oldTopK.remove(gb)) {
                            out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 1, addPs));
                        }else {
                            if (deleteLeafs.remove(gb.leaf)){
                                out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 6, new Global2LocalTID(TID)));
                            }else {
                                out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 3, trackPs));
                                gb.leaf.bitmap.add(TID);
                            }
                        }
                    }
                    outDeleteTopKPTrackGB(TID, oldTopK);
                }
            }
            track.enlargeTuple.f0.bitmap.add(TID);
        }catch (Exception e){
            e.printStackTrace();
        }



    }

    private void recalculateInPointTrack(TrackKeyTID track, List<TrackPoint> points, Rectangle pointsMBR) {
        Integer TID = track.trajectory.TID;
        if (track.enlargeTuple.f0 != track.passP.get(0)) track.enlargeTuple.f0.bitmap.remove(TID);
        Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
        MBR = MBR.getUnionRectangle(pointsMBR);
        double newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
        Rectangle pruneArea = MBR.clone().extendLength(newThreshold);

        //往track经过的节点发送数据
        Global2LocalPoints trackPs = Global2LocalPoints.ToG2LPoints(track.trajectory);
        Global2LocalPoints addPs = new Global2LocalPoints(points);
        track.passP.forEach(dataNode -> out.collect(new Global2LocalElem(dataNode.leafID, (byte) 0, addPs)));
        List<GDataNode> newMBRLeafs = globalTree.getIntersectLeafNodes(pointsMBR);
        newMBRLeafs.removeIf(leaf -> track.passP.contains(leaf));
        if (!newMBRLeafs.isEmpty()){
            if (track.topKP.isEmpty()){
                for (GDataNode leaf : newMBRLeafs) {
                    out.collect(new Global2LocalElem(leaf.leafID, (byte) 2, trackPs));
                    leaf.bitmap.add(TID);
                }
            }else {
                List<GLeafAndBound> topKLeafs = track.topKP.getList();
                for (GDataNode leaf : newMBRLeafs) {
                    GLeafAndBound gb = new GLeafAndBound(leaf, 0.0);
                    if (topKLeafs.contains(gb)) {
                        out.collect(new Global2LocalElem(leaf.leafID, (byte) 1, addPs));
                        out.collect(new Global2LocalElem(leaf.leafID, (byte) 7, new Global2LocalTID(TID)));
                        topKLeafs.remove(gb);
                    } else {
                        out.collect(new Global2LocalElem(leaf.leafID, (byte) 2, trackPs));
                        leaf.bitmap.add(TID);
                    }
                }
            }
            track.passP.addAll(newMBRLeafs);
        }

        if (track.topKP.isEmpty()){ //没有topK扩展节点
            List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
            if (pruneAreaLeafs.size() == track.passP.size()){ //还没有topK扩展节点
                globalTree.countEnlargeBound(track, track.passP, MBR);
                track.rect = pruneArea;
                track.data = pruneArea.getCenter().data;
                track.threshold = newThreshold;
            }else { //有topK扩展节点了
                pruneArea = extendTopKP(track, MBR);
                Rectangle tmpArea = pruneArea;
                pruneAreaLeafs.removeIf(leaf -> !leaf.region.isIntersection(tmpArea));
                track.rect = pruneArea;
                track.data = pruneArea.getCenter().data;
                if (track.passP.size() == pruneAreaLeafs.size()) { //还没有topK扩展节点
                    track.cutOffCandidate(trackMap);
                    globalTree.countEnlargeBound(track, track.passP, MBR);
                }else { //有topK扩展节点了
                    pruneIndex.insert(track);
                    globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                    outAddTopKTrackGB(track.topKP.getList(), trackPs);
                }
            }
        }else { //有topK扩展节点
            pruneArea = DTConstants.recalculateTrackTopK(track, MBR, segmentIndex, trackMap);
            List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
            if (pruneAreaLeafs.size() == track.passP.size()){ //没有topK扩展节点
                track.cutOffCandidate(trackMap);
                outDeleteTopKPTrackGB(TID, track.topKP.getList());
                globalTree.countEnlargeBound(track, track.passP, MBR);
                pruneIndex.delete(track);
                track.topKP.clear();
                track.rect = pruneArea;
                track.data = pruneArea.getCenter().data;
            }else { //有topK扩展节点
                pruneIndex.alterELem(track, pruneArea);
                List<GLeafAndBound> oldTopK = track.topKP.getList();
                globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                for (GLeafAndBound gb : track.topKP.getList()){
                    if (oldTopK.remove(gb)) {
                        out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 1, addPs));
                    }else {
                        out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 3, trackPs));
                        gb.leaf.bitmap.add(TID);
                    }
                }
                outDeleteTopKPTrackGB(TID, oldTopK);
            }
        }
        track.enlargeTuple.f0.bitmap.add(TID);
    }

    private void recalculateOutPointTrack(TrackKeyTID track) {
        try {
            Integer TID = track.trajectory.TID;
            if (track.enlargeTuple.f0 != track.passP.get(0)) track.enlargeTuple.f0.bitmap.remove(TID);
            Rectangle MBR = Constants.getPruningRegion(track.trajectory, 0.0);
            double newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
            Rectangle pruneArea = MBR.clone().extendLength(newThreshold);
            List<GDataNode> deleteLeafs = (List<GDataNode>) Collections.removeAndGatherElms(track.passP, leaf -> !leaf.region.isIntersection(MBR));
            if (track.topKP.isEmpty()){ //没有topK扩展节点
                List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
                if (track.passP.size() == pruneAreaLeafs.size()){ //还没有topK扩展节点
                    globalTree.countEnlargeBound(track, track.passP, MBR);
                    track.rect = pruneArea;
                    track.data = pruneArea.getCenter().data;
                    track.threshold = newThreshold;
                }else { //有topK扩展节点了
                    pruneArea = extendTopKP(track, MBR);
                    Rectangle finalArea = pruneArea;
                    pruneAreaLeafs.removeIf(leaf -> !finalArea.isIntersection(leaf.region));
                    track.rect = pruneArea;
                    track.data = pruneArea.getCenter().data;
                    if (track.passP.size() == pruneAreaLeafs.size()) { //还没有topK扩展节点
                        track.cutOffCandidate(trackMap);
                        globalTree.countEnlargeBound(track, track.passP, MBR);
                    }else { //有topK扩展节点了
                        pruneIndex.insert(track);
                        globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                        Global2LocalPoints trackPoints = Global2LocalPoints.ToG2LPoints(track.trajectory);
                        for (GLeafAndBound gb : track.topKP.getList()) {
                            if (deleteLeafs.remove(gb.leaf)){
                                out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 6, new Global2LocalTID(TID)));
                            }else {
                                out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 3, trackPoints));
                                gb.leaf.bitmap.add(TID);
                            }
                        }
                    }
                }
            }else { //有topK扩展节点
                pruneArea = DTConstants.recalculateTrackTopK(track, MBR, segmentIndex, trackMap);
                List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
                if (pruneAreaLeafs.size() == track.passP.size()){ //没有topK扩展节点
                    track.cutOffCandidate(trackMap);
                    outDeleteTopKPTrackGB(TID, track.topKP.getList());
                    globalTree.countEnlargeBound( track, track.passP, MBR);
                    pruneIndex.delete(track);
                    track.topKP.clear();
                    track.rect = pruneArea;
                    track.data = pruneArea.getCenter().data;
                }else { //有topK扩展节点
                    pruneIndex.alterELem(track, pruneArea);
                    List<GLeafAndBound> oldTopK = track.topKP.getList();
                    globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                    Global2LocalPoints trackPoints = Global2LocalPoints.ToG2LPoints(track.trajectory);
                    for (GLeafAndBound gb : track.topKP.getList()){
                        if (!oldTopK.remove(gb)) {
                            if (deleteLeafs.remove(gb.leaf)){
                                out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 6, new Global2LocalTID(TID)));
                            }else {
                                out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 3, trackPoints));
                                gb.leaf.bitmap.add(TID);
                            }
                        }
                    }
                    outDeleteTopKPTrackGB(TID, oldTopK);
                }
            }
            outDeletePassTrack(TID, deleteLeafs);
            track.enlargeTuple.f0.bitmap.add(TID);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void pruneIndexRemove(TrackKeyTID track){
        pruneIndex.delete(track);
        GLeafAndBound gB = track.topKP.getFirst();
        track.enlargeTuple.f0 = gB.leaf;
        track.enlargeTuple.f1 = gB.bound;
        track.topKP.clear();
    }

    private Rectangle extendTopKP(TrackKeyTID track, Rectangle MBR) {
        Integer TID = track.trajectory.TID;
        SimilarState thresholdState = track.getKCanDistance(Constants.topK + Constants.t);
        double threshold = thresholdState.distance;
        Rectangle pruneArea = MBR.clone().extendLength(threshold);
        //用裁剪域筛选出候选轨迹集，计算距离并排序
        Set<Integer> needCompareTIDS = segmentIndex.getRegionInternalTIDs(pruneArea);
        needCompareTIDS.remove(TID);
        List<SimilarState> needCompareState = new ArrayList<>(needCompareTIDS.size());
        for (Integer compareTid : needCompareTIDS) {
            SimilarState state = track.getSimilarState(compareTid);
            if (state == null)
                state = Hausdorff.getHausdorff(track.trajectory, trackMap.get(compareTid).trajectory);
            needCompareState.add(state);
        }
        java.util.Collections.sort(needCompareState);

        //删除track中原有的无关的RelatedInfo
        for (Iterator<Map.Entry<SimilarState, SimilarState>> ite = track.getRelatedInfo().entrySet().iterator(); ite.hasNext();){
            SimilarState state = ite.next().getKey();
            if (!needCompareState.contains(state)){
                int comparedTID = Constants.getStateAnoTID(state, TID);
                TrackKeyTID comparedTrack = trackMap.get(comparedTID);
                if (!comparedTrack.candidateInfo.contains(TID)) {
                    ite.remove();
                    comparedTrack.removeRelatedInfo(state);
                }
            }
        }

        track.candidateInfo.clear();
        //紧缩裁剪区以查找需要保留相似度计算中间结果的轨迹
        if (needCompareState.indexOf(thresholdState) > Constants.topK + Constants.t * 2 - 1) {
            pruneArea.extendLength(-threshold);
            threshold = needCompareState.get(Constants.topK + Constants.t - 1).distance;
            pruneArea.extendLength(threshold);
            for (int i = 0; i < Constants.topK + Constants.t; i++) {
                SimilarState state = needCompareState.get(i);
                int comparedTID = Constants.getStateAnoTID(state, TID);
                track.candidateInfo.add(comparedTID);
                state = trackMap.get(comparedTID).putRelatedInfo(state);
                track.putRelatedInfo(state);

            }
            Map<SimilarState, SimilarState> map = track.getRelatedInfo();
            for (int i = Constants.topK + Constants.t; i < needCompareState.size(); i++) {
                SimilarState state = needCompareState.get(i);
                Integer comparedTID = Constants.getStateAnoTID(state, TID);
                TrackKeyTID comparedTrack = trackMap.get(comparedTID);
                if (comparedTrack.outSideRectangle(pruneArea)) {
                    if (map.containsKey(state) && !comparedTrack.candidateInfo.contains(TID)) {
                        track.removeRelatedInfo(state);
                        comparedTrack.removeRelatedInfo(state);
                    }
                } else {
                    track.candidateInfo.add(comparedTID);
                    state = comparedTrack.putRelatedInfo(state);
                    track.putRelatedInfo(state);

                }
            }
        } else {
            for (SimilarState state : needCompareState) {
                Integer comparedTID = Constants.getStateAnoTID(state, TID);
                track.candidateInfo.add(comparedTID);
                state = trackMap.get(comparedTID).putRelatedInfo(state);
                track.putRelatedInfo(state);
            }
        }
        track.threshold = threshold;
        return pruneArea;
    }

    /**
     * 轨迹的阈值可能发生变化时，调用该函数，进行修改阈值的一系列操作
     */
    private void changeThreshold(TrackKeyTID track) {
        double dis = track.getKCanDistance(Constants.topK).distance;
        Integer TID = track.trajectory.TID;
        if (dis > track.threshold) { //需扩大阈值
            dis = track.getKCanDistance(Constants.topK + Constants.t).distance;
            Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
            if (track.topKP.isEmpty()) { //无topK扩展节点
                if (dis > track.enlargeTuple.f1) { //无topK扩展节点，现在需要扩展
                    track.rect = extendTopKP(track, MBR);
                    List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(track.rect);
                    if (track.passP.get(0) != track.enlargeTuple.f0) track.enlargeTuple.f0.bitmap.remove(TID);
                    if (track.passP.size() == pruneAreaLeafs.size()) { //还没有topK扩展节点
                        track.cutOffCandidate(trackMap);
                        globalTree.countEnlargeBound(track, track.passP, MBR);
                    }else { //有topK扩展节点了
                        pruneIndex.insert(track);
                        globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                        outAddTopKTrackGB(track.topKP.getList(), Global2LocalPoints.ToG2LPoints(track.trajectory));
                    }
                    track.enlargeTuple.f0.bitmap.add(TID);
                }else {  //无topK扩展节点，现在不需要扩展
                    track.rect.extendLength(-track.threshold + dis);
                    track.threshold = dis;
                }
            }else{  //有topK扩展节点
                if (track.passP.get(0) != track.enlargeTuple.f0) track.enlargeTuple.f0.bitmap.remove(TID);
                Tuple2<Boolean, Rectangle> tuple2 = DTConstants.enlargePrune(track, dis, -1, segmentIndex, trackMap);
                if (track.enlargeTuple.f1 < track.threshold) { //topK扩展节点变多
                    pruneIndex.alterELem(track, tuple2.f1);
                    List<GDataNode> leafs = globalTree.enlargePartitions(track, MBR);
                    //通知扩展
                    outAddTopKTrackGD(leafs, Global2LocalPoints.ToG2LPoints(track.trajectory));
                }else{
                    if (track.topKP.getLast().bound >= track.threshold) { //topK扩展节点变少
                        if (track.topKP.getFirst().bound < track.threshold){
                            pruneIndex.alterELem(track, tuple2.f1);
                            List<GLeafAndBound> minusTopKP = track.minusTopKP();
                            outDeleteTopKPTrackGB(TID, minusTopKP);
                        }else {  //topK扩展节点变没
                            outDeleteTopKPTrackGB(TID, track.topKP.getList());
                            pruneIndexRemove(track);
                            track.rect = tuple2.f1;
                            track.cutOffCandidate(trackMap, -1, null);
                        }
                    }else {
                        pruneIndex.alterELem(track, tuple2.f1);
                    }
                }
                track.enlargeTuple.f0.bitmap.add(TID);
            }
            return;
        }

        dis = track.getKCanDistance(Constants.topK + Constants.t * 2).distance;
        if (dis < track.threshold) { //需缩小阈值
            if (track.topKP.isEmpty()){ //没有topK扩展节点
                double newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
                track.rect.extendLength(newThreshold - track.threshold);
                track.threshold = newThreshold;
            }else { //有topK扩展节点
                track.enlargeTuple.f0.bitmap.remove(TID);
                Rectangle pruneArea = DTConstants.tightenThresholdCommon(track, -1, null, trackMap);
                if (track.threshold < track.topKP.getLast().bound){ //topK扩展节点变少
                    if (track.threshold < track.topKP.getFirst().bound){ //topK扩展节点变没
                        outDeleteTopKPTrackGB(TID, track.topKP.getList());
                        pruneIndexRemove(track);
                        track.rect = pruneArea;
                        track.cutOffCandidate(trackMap, -1, null);
                    }else { //topK扩展节点变少
                        pruneIndex.alterELem(track, pruneArea);
                        List<GLeafAndBound> minusTopKP = track.minusTopKP();
                        outDeleteTopKPTrackGB(TID, minusTopKP);
                    }
                }else {
                    pruneIndex.alterELem(track, pruneArea);
                }
                track.enlargeTuple.f0.bitmap.add(TID);
            }
        }
    }

    /**
     * 轨迹track可能成为其它轨迹的topK结果
     */
    private void mayBeAnotherTopK(TrackKeyTID track) {
        Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
        List<Integer> list = pruneIndex.trackInternal(MBR);
        Integer TID = track.trajectory.TID;
        list.remove(TID);
        for (Integer comparedTid : list) {
            TrackKeyTID comparedTrack = trackMap.get(comparedTid);
            if (!comparedTrack.candidateInfo.contains(TID)){
                Constants.addTrackCandidate(comparedTrack, track);
                comparedTrack.updateCandidateInfo(TID);
                changeThreshold(comparedTrack);
            }
        }
    }

    /**
     * 通知removeList中的对应的divide节点删除topK轨迹comparedTid
     */
    private void outDeleteTopKPTrackGB(Integer TID, List<GLeafAndBound> removeList) {
        removeList.forEach(gB -> {
            gB.leaf.bitmap.remove(TID);
            out.collect(new Global2LocalElem(gB.leaf.leafID, (byte) 5, new Global2LocalTID(TID)));
        });
    }


    private void initCalculate(){
        for (TrackKeyTID track : trackMap.values()) {
            int TID = track.trajectory.TID;
            List<TrackKeyTID> list = new ArrayList<>(trackMap.values());
            list.remove(track);
            for (TrackKeyTID comparedTrack : list)
                Constants.addTrackCandidate(track, comparedTrack);
            track.sortCandidateInfo();
            track.threshold = track.getKCanDistance(Constants.t + Constants.topK).distance;
            Rectangle MBR = track.rect.clone();
            track.rect = MBR.clone().extendLength(track.threshold);
            globalTree.countPartitions(MBR, track);
            track.enlargeTuple.f0.bitmap.add(TID);
            Global2LocalPoints trackPs = Global2LocalPoints.ToG2LPoints(track.trajectory);
            if (!track.topKP.isEmpty()) {
                outAddTopKTrackGB(track.topKP.getList(), trackPs);
                pruneIndex.insert(track);
            }
            for (int i = Constants.t + Constants.topK; i < track.candidateInfo.size(); i++) {
                TrackKeyTID comparedTrack = trackMap.get(track.candidateInfo.get(i));
                if (comparedTrack.outSideRectangle(track.rect)){
                    track.removeTIDCandidate(comparedTrack);
                    i--;
                }
            }
            outAddPassTrack(track.passP, trackPs);
        }
    }

    /**
     * 通知list中的对应的divide节点,添加一条新的topK轨迹trackMessage
     */
    private void outAddTopKTrackGB(List<GLeafAndBound> list, Global2LocalPoints track) {
        int TID = track.points.get(0).TID;
        list.forEach(gB -> {
            out.collect(new Global2LocalElem(gB.leaf.leafID, (byte) 3, track));
            gB.leaf.bitmap.add(TID);
        });
    }

    /**
     * 通知list中的对应的divide节点,添加一条新的topK轨迹trackMessage
     */
    private void outAddTopKTrackGD(Collection<GDataNode> leafs, Global2LocalPoints track) {
        int TID = track.points.get(0).TID;
        leafs.forEach(leaf -> {
            out.collect(new Global2LocalElem(leaf.leafID, (byte) 3, track));
            leaf.bitmap.add(TID);
        });
    }

    /**
     * 通知list中的对应的divide节点,添加一条新的路过轨迹trackMessage
     */
    private void outAddPassTrack(List<GDataNode> list, Global2LocalPoints track) {
        int TID = track.points.get(0).TID;
        list.forEach(dataNode -> {
            out.collect(new Global2LocalElem(dataNode.leafID, (byte) 2, track));
            dataNode.bitmap.add(TID);
        });
    }

    /**
     * 通知removeList中的对应的divide节点删除topK轨迹comparedTid
     */
    private void outDeletePassTrack(int TID, List<GDataNode> leafs) {
        Global2LocalTID p = new Global2LocalTID(TID);
        leafs.forEach(leaf -> {
            leaf.bitmap.remove(TID);
            out.collect(new Global2LocalElem(leaf.leafID, (byte) 4, p));
        });
    }

    /**
     * 通知removeList中的对应的divide节点删除topK轨迹comparedTid
     */
    private void outDeleteTopKPTrackGD(Integer TID, List<GDataNode> removeList) {
        Global2LocalTID p = new Global2LocalTID(TID);
        removeList.forEach(leaf -> {
            leaf.bitmap.remove(TID);
            out.collect(new Global2LocalElem(leaf.leafID, (byte) 5, p));
        });
    }

    /**
     * 对窗口输入数据elements进行预处理，包括识别并统计出密度网格、将轨迹
     * 点按照轨迹ID分类到Map中、统计所有的轨迹ID到RoaringBitmap中、将轨
     * 迹点添加到轨迹中。
     */
    private int[][] preElements(Iterable<Density2GlobalElem> elements,
                                List<TrackKeyTID> newTracks,
                                Map<Integer , List<TrackPoint>> inPointsMap) {
        List<Density> densities = new ArrayList<>(Constants.globalPartition);
        for (Density2GlobalElem element : elements) {
            if (element instanceof TrackPoint){
                TrackPoint point = (TrackPoint) element;
                inPointsMap.computeIfAbsent(point.TID, tid -> new ArrayList<>(4)).add(point);
            }else {
                densities.add((Density) element);
            }
        }
        int[][] density = null;
        if (!densities.isEmpty()){
            density = new int[Constants.gridDensity+1][Constants.gridDensity+1];
            for (Density elem : densities) Arrays.addArrsToArrs(density, elem.grids, true);
        }

        Iterator<Map.Entry<Integer, List<TrackPoint>>> ite = inPointsMap.entrySet().iterator();
        while (ite.hasNext()){
            Map.Entry<Integer, List<TrackPoint>> entry = ite.next();
            TrackKeyTID track = trackMap.get(entry.getKey());
            List<Segment> segments;
            if (track == null){ //trackMap中没有的轨迹点
                TrackPoint prePoint = singlePointMap.remove(entry.getKey());
                if (prePoint == null){
                    if (entry.getValue().size() == 1){ //新的轨迹点，且只有一个点。
                        singlePointMap.put(entry.getKey(), entry.getValue().get(0));
                        ite.remove();
                        continue;
                    }
                } else {
                    entry.getValue().add(0, prePoint);
                }
                segments = Segment.pointsToSegments(entry.getValue());
                Rectangle rect = Rectangle.getUnionRectangle(com.ada.common.collections.Collections.changeCollectionElem(segments, seg -> seg.rect).toArray(new Rectangle[]{}));
                track = new TrackKeyTID(null,
                        rect.getCenter().data,
                        rect,
                        new ArrayQueue<>(segments),
                        entry.getKey(),
                        new ArrayList<>(),
                        new HashMap<>());
                trackMap.put(entry.getKey(), track);
                newTracks.add(track);
            }else { //已有的轨迹
                entry.getValue().add(0, track.trajectory.elms.getLast().p2);
                segments = Segment.pointsToSegments(entry.getValue());
                track.trajectory.addSegments(segments);
            }
            for (Segment segment : segments) segmentIndex.insert(segment);
        }
        return density;
    }


    private void adjustLocalTasksRegion(Map<GNode, GNode> nodeMap){
//        Map<Integer, LocalRegionAdjustInfo> migrateOutMap = new HashMap<>();
//        Map<Integer, LocalRegionAdjustInfo> migrateFromMap = new HashMap<>();
//        for (Map.Entry<GNode, GNode> entry : nodeMap.entrySet()) {
//            List<GDataNode> leafs = new ArrayList<>();
//            entry.getKey().getLeafs(leafs);
//            for (GDataNode oldLeaf : leafs) {
//                List<GDataNode> migrateOutLeafs = new ArrayList<>();
//                entry.getValue().getIntersectLeafNodes(oldLeaf.region, migrateOutLeafs);
//                migrateOutLeafs.removeIf(leaf -> leaf.leafID == oldLeaf.leafID);
//                List<Tuple2<Integer, Rectangle>> migrateOutLeafIDs = (List<Tuple2<Integer, Rectangle>>) com.ada.common.collections.Collections.changeCollectionElem(migrateOutLeafs, node -> new Tuple2<>(node.leafID,node.region));
//                migrateOutMap.put(oldLeaf.leafID, new LocalRegionAdjustInfo(migrateOutLeafIDs, null, null));
//            }
//            leafs.clear();
//            entry.getValue().getLeafs(leafs);
//            for (GDataNode newLeaf : leafs) {
//                List<GDataNode> migrateFromLeafs = new ArrayList<>();
//                entry.getKey().getIntersectLeafNodes(newLeaf.region, migrateFromLeafs);
//                List<Integer> migrateFromLeafIDs = (List<Integer>) com.ada.common.collections.Collections.changeCollectionElem(migrateFromLeafs, node -> node.leafID);
//                migrateFromLeafIDs.remove(new Integer(newLeaf.leafID));
//                migrateFromMap.put(newLeaf.leafID, new LocalRegionAdjustInfo(null, migrateFromLeafIDs, newLeaf.region));
//            }
//        }
//        Iterator<Map.Entry<Integer, LocalRegionAdjustInfo>> ite = migrateOutMap.entrySet().iterator();
//        while (ite.hasNext()){
//            Map.Entry<Integer, LocalRegionAdjustInfo> entry = ite.next();
//            LocalRegionAdjustInfo elem = migrateFromMap.get(entry.getKey());
//            if (elem != null){
//                elem.setMigrateOutST(entry.getValue().getMigrateOutST());
//                ite.remove();
//            }
//        }
//        migrateFromMap.putAll(migrateOutMap);
//        for (Map.Entry<Integer, LocalRegionAdjustInfo> entry : migrateFromMap.entrySet())
//            out.collect(new GlobalToLocalElem(entry.getKey(), 3, entry.getValue()));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        subTask = getRuntimeContext().getIndexOfThisSubtask();
        densityQue = new ArrayDeque<>();
        globalTree = new GTree();
        hasInit = false;
        tIDsQue = new ArrayDeque<>();
        trackMap = new HashMap<>();
        singlePointMap = new HashMap<>();
        segmentIndex = new RCtree<>(4,1,11, Constants.globalRegion,0, true);
        pruneIndex = new RCtree<>(4,1,11, Constants.globalRegion,0, false);
        count = 0;
    }
}
