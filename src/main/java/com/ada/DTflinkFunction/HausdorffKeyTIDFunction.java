package com.ada.DTflinkFunction;

import com.ada.globalTree.GDataNode;
import com.ada.globalTree.GDirNode;
import com.ada.globalTree.GNode;
import com.ada.globalTree.GTree;
import com.ada.QBSTree.RCtree;
import com.ada.common.Arrays;
import com.ada.common.Constants;
import com.ada.common.SortList;
import com.ada.model.*;
import com.ada.geometry.*;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.jetbrains.annotations.NotNull;
import org.roaringbitmap.RoaringBitmap;
import java.util.*;

public class HausdorffKeyTIDFunction extends RichFlatMapFunction<OneTwoData, TwoThreeData> {
    private int subTask;
    private boolean hasInit;
    private GTree globalTree;
    private Queue<Tuple3<Long, int[][], RoaringBitmap>> densities;

    private Map<Long, Integer> waterMap;
    private long curWater;
    private long startWindow;
    private Map<Long, int[][]> densityMap;
    private LinkedList<TrackPoint> pointQueue;
    private Comparator<TrackPoint> comparator;

    private Map<Integer, TrackKeyTID> trackMap;
    private Map<Integer, TrackPoint> singlePointMap;
    private RCtree<Segment> pointIndex;
    private RCtree<TrackKeyTID> pruneIndex;
    private RoaringBitmap TIDs;
    private Collector<TwoThreeData> out;
    private int count;

    @Override
    public void flatMap(OneTwoData value, Collector<TwoThreeData> out){
        this.out = out;
        if (value instanceof OneTwoPoint){ //点
            DTConstants.addOnePointQueue(pointQueue, ((OneTwoPoint) value).point, comparator);
        }else if (value instanceof OneTwoWater){ //水印
            Long water = ((OneTwoWater) value).water;
            Integer wNum = waterMap.computeIfAbsent(water, aLong -> 0);
            wNum++;
            if (wNum == Constants.inputPartition){
                waterMap.remove(water);
                curWater = water;
                List<TrackPoint> points = removePointQueue(curWater);
                //处理点
                processPoint(points);
                long minus = curWater - startWindow;
                if ( minus >= Constants.windowSize ){
                    startWindow = startWindow + ((int) (minus/Constants.windowSize))*Constants.windowSize;
                }else {
                    for (int i = 0; i < Constants.dividePartition; i++)
                        out.collect(new TwoThreeWater(i, water));
                }
            }else {
                waterMap.replace(water, wNum);
            }
        }else { //密度网格
            OneTwoDensity oneTwoDensity = (OneTwoDensity) value;
            long curStartWin = oneTwoDensity.timeStamp - Constants.windowSize;
            int[][] aDensity = densityMap.get(curStartWin);
            if (aDensity == null) {
                aDensity = oneTwoDensity.density;
                densityMap.put(curStartWin, aDensity);
            }else
                Arrays.addArrsToArrs(aDensity, oneTwoDensity.density, true);
            if(curWater >= curStartWin){
                densityMap.remove(curStartWin);
                densities.add(new Tuple3<>(curStartWin,aDensity,TIDs));
                TIDs = new RoaringBitmap();
                Tuple3<Long, int[][], RoaringBitmap> tuple3 = densities.element();
                long logicStartWin = oneTwoDensity.timeStamp - (Constants.windowSize*Constants.logicWindow);
                if (tuple3.f0 < logicStartWin){
                    densities.poll();
                    globalTree.deleteOldDensity(tuple3.f1);
                    windowSlide(tuple3.f2, logicStartWin);
                }
                globalTree.addNewDensity(aDensity);
                Map<GNode, GNode> map = globalTree.updateTree();
                if (!map.isEmpty()){
                    if (subTask == 0) {
                        //通知Local Index其索引区域发生的变化
                        for (GDataNode leaf : globalTree.newLeafs)
                            out.collect(new TwoThreeAdjRegion(leaf.leafID, leaf.rectangle, oneTwoDensity.timeStamp));
                        //通知弃用的子节点
                        for (Integer discardLeafID : globalTree.discardLeafIDs)
                            out.collect(new TwoThreeAdjRegion(discardLeafID, null, oneTwoDensity.timeStamp));
                    }
                    redispatchGNode(map);
                    check();
                    globalTree.discardLeafIDs.clear();
                    globalTree.newLeafs.clear();
                }
                for (int i = 0; i < Constants.dividePartition; i++)
                    out.collect(new TwoThreeWater(i, oneTwoDensity.timeStamp));
            }

        }
    }

    private boolean check(){
        pruneIndex.check(trackMap);
        pointIndex.check(trackMap);
        globalTree.check(trackMap);
        trackMap.values().forEach(this::trackCheck);
        return true;
    }

    private void trackCheck(TrackKeyTID track){
        Integer TID = track.trajectory.TID;

        //trajectory.elems -- pointIndex检查
        for (Segment segment : track.trajectory.elems) {
            if (!segment.leaf.elms.contains(segment))
                throw new IllegalArgumentException(segment.obtainTID() + " " + segment.p1);
        }

        //RelatedInfo 检查
        for (SimilarState key : track.getRelatedInfo().keySet()) {
            SimilarState state = track.getRelatedInfo().get(key);
            int comparedTID = Constants.getStateAnoTID(state, TID);
            TrackHauOne comparedTrack = trackMap.get(comparedTID);
            if (key != state)
                throw new IllegalArgumentException(TID + " " + comparedTID);
            if (comparedTrack.getSimilarState(TID) != state)
                throw new IllegalArgumentException(TID + " " + comparedTID);
            SimilarState state1 = Constants.getHausdorff(track.trajectory, comparedTrack.trajectory);
            if(!Constants.isEqual(state.distance, state1.distance))
                throw new IllegalArgumentException(TID + " " + comparedTID +
                        " " + state.distance + " " + state1.distance);
            if (!comparedTrack.candidateInfo.contains(TID) && !track.candidateInfo.contains(comparedTID))
                throw new IllegalArgumentException(TID + " " + comparedTID);
        }

        //candidateInfo 检查
        if (track.candidateInfo.size() < Constants.topK)
            throw new IllegalArgumentException(TID + " " + track.candidateInfo.size());
        for (Integer comparedTID : track.candidateInfo) {
            if(track.getSimilarState(comparedTID) == null)
                throw new IllegalArgumentException(TID + " " + comparedTID);
        }
        for (int i = 0; i < track.candidateInfo.size()-1; i++) {
            SimilarState state1 = track.getSimilarState(track.candidateInfo.get(i));
            SimilarState state2 = track.getSimilarState(track.candidateInfo.get(i+1));
            if (Double.compare(state1.distance,state2.distance) > 0)
                throw new IllegalArgumentException(TID + " " + Constants.getStateAnoTID(state1, TID)
                        + " " + Constants.getStateAnoTID(state2, TID));
        }


        Rectangle MBR = Constants.getPruningRegion(track.trajectory, 0.0);
        Rectangle pruneArea = MBR.clone().extendLength(track.threshold);

        //rect 检查
        if (!pruneArea.equals(track.rect))
            throw new IllegalArgumentException(TID + " " + track.rect + " " + pruneArea);

        //重新计算
        TrackKeyTID tmpTrack = new TrackKeyTID(null, null, null, null ,TID,null, null);
        Set<Integer> selectedTIDs;  //阈值计算轨迹集
        if (count == 8493 && TID == 34)
            System.out.print("");
        selectedTIDs = pointIndex.getRegionInternalTIDs(pruneArea);
        selectedTIDs.remove(TID);
        if (selectedTIDs.size() < Constants.topK)
            throw new IllegalArgumentException(TID + " selectedTIDs.size() < Constants.topK");
        List<SimilarState> result = new  ArrayList<>();
        for (Integer comparedTid : selectedTIDs) {
            TrackKeyTID comparedTrack = trackMap.get(comparedTid);
            SimilarState state = track.getSimilarState(comparedTid);
            if (state == null)
                state = Constants.getHausdorff(track.trajectory, comparedTrack.trajectory);
            result.add(state);
        }
        Collections.sort(result);

        tmpTrack.threshold = result.get(Constants.topK - 1).distance;

        //threshold 检查
        if (track.threshold < tmpTrack.threshold ||
                track.threshold < track.getKCanDistance(Constants.topK).distance ||
                track.getKCanDistance(Constants.topK + Constants.t * 2).distance < track.threshold) {
            throw new IllegalArgumentException("threshold error");
        }else {
            tmpTrack.rect = pruneArea;
            tmpTrack.threshold = track.threshold;
        }
        globalTree.countPartitions(MBR, tmpTrack);

        if (!Constants.isEqual(track.enlargeTuple.f1, tmpTrack.enlargeTuple.f1))
            throw new IllegalArgumentException(TID + "enlargeTuple error");

        if (!track.enlargeTuple.f0.bitmap.contains(TID))
            throw new IllegalArgumentException(TID + "enlargeTuple error");

        if (!track.passP.containsAll(tmpTrack.passP) || !tmpTrack.passP.containsAll(track.passP))
            throw new IllegalArgumentException(TID + "passP error");
        for (GDataNode passLeaf : track.passP) {
            if (!passLeaf.bitmap.contains(TID))
                throw new IllegalArgumentException(TID + "passP error");
        }


        Rectangle rect = track.enlargeTuple.f0.rectangle.extendToEnoughBig();
        if (rect.isIntersection(track.rect)){
            if (!(!rect.isInternal(MBR.clone().extendLength(track.enlargeTuple.f1 + 0.0002)) &&
                    rect.isInternal(MBR.clone().extendLength(track.enlargeTuple.f1 - 0.0002))))
                throw new IllegalArgumentException(TID + " enlargeTuple bound error");
        }else {
            if ( !(rect.isIntersection(MBR.clone().extendLength(track.enlargeTuple.f1 + 0.0002)) &&
                    !rect.isIntersection(MBR.clone().extendLength(track.enlargeTuple.f1 - 0.0002))))
                throw new IllegalArgumentException(TID + " enlargeTuple bound error");
        }



        if (tmpTrack.topKP.isEmpty()){
            if (!track.topKP.isEmpty())
                throw new IllegalArgumentException(TID + " !track.topKP.isEmpty()");
            if (track.threshold > tmpTrack.enlargeTuple.f1)
                throw new IllegalArgumentException(TID + " track.threshold > tmpTrack.enlargeTuple.f1");
            if (track.threshold < tmpTrack.threshold)
                throw new IllegalArgumentException(TID + " track.threshold < tmpTrack.threshold");
            if(track.leaf != null) //pruneIndex检查
                throw new IllegalArgumentException(TID + " track.leaf != null");
            if (track.getKCanDistance(Constants.topK).distance > track.threshold)
                throw new IllegalArgumentException(TID + " candidateInfo error");

        }else {
            if (track.topKP.isEmpty())
                throw new IllegalArgumentException(TID + "track.topKP.isEmpty()");
            if (!track.topKP.getList().containsAll(tmpTrack.topKP.getList()) ||
                    !tmpTrack.topKP.getList().containsAll(track.topKP.getList()))
                throw new IllegalArgumentException(TID + "topKP error");
            for (GLeafAndBound gb : track.topKP.getList()) {
                GLeafAndBound gbb = tmpTrack.topKP.get(gb);
                if (!Constants.isEqual(gb.bound, gbb.bound))
                    throw new IllegalArgumentException(TID + "topKP bound error");
                if (!gb.leaf.bitmap.contains(TID))
                    throw new IllegalArgumentException(TID + "topKP bound error");
            }
            if(!track.leaf.elms.contains(track)) //pruneIndex检查
                throw new IllegalArgumentException(TID + " !track.leaf.elms.contains(track)");
            if (!selectedTIDs.containsAll(track.candidateInfo) ||
                    !track.candidateInfo.containsAll(selectedTIDs))
                throw new IllegalArgumentException(TID + " candidateInfo error");
            for (GLeafAndBound gb : track.topKP.getList()) {
                if ( !(gb.leaf.rectangle.isIntersection(MBR.clone().extendLength(gb.bound + 0.0002)) &&
                        !gb.leaf.rectangle.isIntersection(MBR.clone().extendLength(gb.bound - 0.0002))))
                    throw new IllegalArgumentException(TID + " enlargeTuple bound error");
            }
        }
    }

    private void redispatchGNode(Map<GNode, GNode> map) {
        //记录轨迹的enlarge node bound在在修改节点集中的轨迹
        RoaringBitmap enlargeTIDs = new RoaringBitmap();

        //记录轨迹在调整后的节点newNode中的经过节点和topK节点
        //key：轨迹ID
        //value：该轨迹需要出现的节点{tuple0:为经过节点，tuple1为topK节点}
        Map<Integer, Tuple2<List<GDataNode>,List<GDataNode>>> trackLeafsMap = new HashMap<>();
        map.forEach((oldNode, newNode) -> {
            List<GDataNode> oldLeafs = oldNode.getLeafs();
            List<GDirNode> dirNodes = new ArrayList<>();
            oldNode.getAllDirNode(dirNodes);
            for (GDirNode dirNode : dirNodes)
                enlargeTIDs.or(dirNode.bitmap);
            for (GDataNode oldLeaf : oldLeafs) {
                for (Integer TID : oldLeaf.bitmap) {
                    TrackKeyTID track = trackMap.get(TID);
                    if(track.enlargeTuple.f0.equals(oldLeaf))
                        enlargeTIDs.add(TID);
                }
            }
        });
        map.forEach((oldNode, newNode) -> {
            Map<Integer, Tuple2<List<GDataNode>,List<GDataNode>>> trackLeafsInnerMap = new HashMap<>();
            List<GDataNode> oldLeafs = oldNode.getLeafs();
            List<GDataNode> newLeafs = newNode.getLeafs();
            for (GDataNode oldLeaf : oldLeafs) {
                for (Integer TID : oldLeaf.bitmap) {
                    TrackKeyTID track = trackMap.get(TID);
                    if (TID == 136)
                        System.out.print("");
                    //轨迹track在newNode子树上的经过叶节点和topK叶节点
                    trackLeafsInnerMap.computeIfAbsent(TID, key -> {
                        Tuple2<List<GDataNode>,List<GDataNode>> value = new Tuple2<>(new ArrayList<>(), new ArrayList<>());
                        Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
                        newNode.getIntersectLeafNodes(MBR, value.f0);
                        newNode.getIntersectLeafNodes(track.rect, value.f1);
                        value.f1.forEach(leaf -> leaf.bitmap.add(TID));
                        if (!enlargeTIDs.contains(TID)) {
                            Set<GDataNode> newLeafsSet = new HashSet<>(newLeafs);
                            newLeafsSet.removeAll(value.f1);
                            Tuple2<GNode, Double> tuple = DTConstants.countEnlarge(newLeafsSet, MBR);
                            if (track.enlargeTuple.f1 > tuple.f1) {
                                track.enlargeTuple.f0.bitmap.remove(TID);
                                track.enlargeTuple = tuple;
                                track.enlargeTuple.f0.bitmap.add(TID);
                            }
                        }
                        value.f1.removeAll(value.f0);
                        return value;
                    });
                }
            }
            trackLeafsInnerMap.forEach((TID, listTuple2) -> {
                Tuple2<List<GDataNode>,List<GDataNode>> trackLeafsOuter = trackLeafsMap.get(TID);
                if (trackLeafsOuter == null){
                    trackLeafsMap.put(TID, listTuple2);
                }else {
                    trackLeafsOuter.f0.addAll(listTuple2.f0);
                    trackLeafsOuter.f1.addAll(listTuple2.f1);
                }
            });
        });


        Collection<GDataNode> allLeafs = globalTree.leafIDMap.values();
        trackLeafsMap.forEach((TID, trackLeafs) -> {
            if (TID == 136)
                System.out.print("");
            TrackKeyTID track = trackMap.get(TID);
            Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
            TrackPoint point =  new TrackPoint(null, curWater-1, TID);
            TrackMessage message = toTrackMessage(track.trajectory);
            if (track.topKP.isEmpty() && trackLeafs.f1.isEmpty()){ //修改前后都没有topK节点
                bothHasNoTopKNode(allLeafs, trackLeafs, track, point, message);
            }else if (!track.topKP.isEmpty() && !trackLeafs.f1.isEmpty()){ //修改前后都有topK节点
                bothHasTopKNode(allLeafs, trackLeafs, track, MBR, point, message);
            }else if(track.topKP.isEmpty() && !trackLeafs.f1.isEmpty()) { //调整后track有了topK节点
                topKAppear(allLeafs, trackLeafs, track, MBR, point, message);
            }else { //调整后track没了topK节点
                topKDisappear(allLeafs, trackLeafs, track, MBR, point, message);
            }
        });

        for (Integer TID : enlargeTIDs) {
            TrackKeyTID track = trackMap.get(TID);
            List<GDataNode> removeLeafs = new ArrayList<>(track.passP);
            if (!track.topKP.isEmpty()){
                for (GLeafAndBound gb : track.topKP.getList())
                    removeLeafs.add(gb.leaf);
            }
            List<GDataNode> pruneLeafs = globalTree.getIntersectLeafNodes(track.rect);
            if (!removeLeafs.containsAll(pruneLeafs) || !pruneLeafs.containsAll(removeLeafs))
                System.out.print("");
            globalTree.countEnlargeBound(track, removeLeafs, track.rect.clone().extendLength(-track.threshold));
            track.enlargeTuple.f0.bitmap.add(TID);
        }
    }

    private void topKAppear(Collection<GDataNode> allLeafs,
                            Tuple2<List<GDataNode>, List<GDataNode>> trackLeafs,
                            TrackKeyTID track,
                            Rectangle MBR,
                            TrackPoint point,
                            TrackMessage message) {
        int TID = track.trajectory.TID;
        track.enlargeTuple.f0.bitmap.remove(TID);
        track.rect = extendTopKP(track, MBR,-1, null);
        SortList<GLeafAndBound> gbs = new SortList<>();
        gbs.setList(new ArrayList<>());
        List<GDataNode> removeLeafs = new ArrayList<>(trackLeafs.f0);
        for (GDataNode topKLeaf : trackLeafs.f1) {
            if (track.rect.isIntersection(topKLeaf.rectangle)){
                double bound = Constants.countEnlargeBound(MBR, topKLeaf.rectangle);
                gbs.add(new GLeafAndBound(topKLeaf, bound) );
                removeLeafs.add(topKLeaf);
            }else {
                topKLeaf.bitmap.remove(TID);
            }
        }
        if (gbs.isEmpty()) { //还没有topK扩展节点
            bothHasNoTopKNode(allLeafs, trackLeafs, track, point, message);
            track.cutOffCandidate(trackMap, -1, null);
        }else { //有topK扩展节点了
            pruneIndex.insert(track);
            track.topKP.setList(new ArrayList<>());
            int i = 0;
            for (Iterator<GDataNode> ite = track.passP.iterator(); ite.hasNext();) {
                GDataNode oldLeaf = ite.next();
                if (!allLeafs.contains(oldLeaf)){ //该pass节点发生了调整
                    GDataNode newLeaf = Constants.removeElem(trackLeafs.f0, leaf -> leaf.leafID == oldLeaf.leafID);
                    if (newLeaf == null){ //调整后轨迹track不经过这个leafID了
                        GLeafAndBound gLB = Constants.removeElem(gbs.getList(), gb -> gb.leaf.leafID == oldLeaf.leafID);
                        if (gLB == null){ //调整后轨迹track与这个leafID无关了
                            out.collect(new TwoThreeTrackInfo(oldLeaf.leafID, (byte) 8, point));
                        }else { //这个leafID在调整后成为轨迹track的topK节点了
                            track.topKP.add(gLB);
                            out.collect(new TwoThreeTrackInfo(oldLeaf.leafID, (byte) 6, point));
                        }
                        i--;
                        ite.remove();
                    }else { //调整后轨迹track还经过这个leafID了
                        track.passP.set(i, newLeaf);
                    }
                }
                i++;
            }
            trackLeafs.f0.forEach(dataNode -> out.collect(new TwoThreeTrackInfo(dataNode.leafID, (byte) 10, message)));
            track.passP.addAll(trackLeafs.f0);
            gbs.getList().forEach(gb -> out.collect(new TwoThreeTrackInfo(gb.leaf.leafID, (byte) 11, message)));
            track.topKP.addAll(gbs.getList());
        }
        globalTree.countEnlargeBound(track, removeLeafs,MBR);
        track.enlargeTuple.f0.bitmap.add(TID);
    }

    private void topKDisappear(Collection<GDataNode> allLeafs,
                               Tuple2<List<GDataNode>, List<GDataNode>> trackLeafs,
                               TrackKeyTID track,
                               Rectangle MBR,
                               TrackPoint point,
                               TrackMessage message) {
        int i = 0;
        for (Iterator<GDataNode> ite = track.passP.iterator(); ite.hasNext();) {
            GDataNode oldLeaf = ite.next();
            if (!allLeafs.contains(oldLeaf)){ //该叶节点发生了调整
                GDataNode newLeaf = Constants.removeElem(trackLeafs.f0, leaf -> leaf.leafID == oldLeaf.leafID);
                if (newLeaf == null){ //调整后轨迹track不经过这个leafID了
                    out.collect(new TwoThreeTrackInfo(oldLeaf.leafID, (byte) 8, point));
                    i--;
                    ite.remove();
                }else { //调整后轨迹track还经过这个leafID了
                    track.passP.set(i, newLeaf);
                }
            }
            i++;
        }
        for (Iterator<GLeafAndBound> ite = track.topKP.getList().iterator(); ite.hasNext();) {
            GLeafAndBound gb = ite.next();
            GDataNode newLeaf = Constants.removeElem(trackLeafs.f0, leaf -> leaf.leafID == gb.leaf.leafID);
            if (newLeaf != null){
                ite.remove();
                out.collect(new TwoThreeTrackInfo(newLeaf.leafID, (byte) 7, point));
                track.passP.add(newLeaf);
            }
        }
        trackLeafs.f0.forEach(dataNode -> out.collect(new TwoThreeTrackInfo(dataNode.leafID, (byte) 10, message)));
        track.passP.addAll(trackLeafs.f0);
        track.topKP.getList().forEach(gB -> out.collect(new TwoThreeTrackInfo(gB.leaf.leafID, (byte) 9, point)));

        if (track.topKP.isEmpty()){
            pruneIndex.delete(track);
            globalTree.countEnlargeBound(track, track.passP, MBR);
            track.topKP.clear();
        }else {
            pruneIndexRemove(track);
        }
        track.cutOffCandidate(trackMap);
    }

    private void bothHasTopKNode(Collection<GDataNode> allLeafs,
                                 Tuple2<List<GDataNode>, List<GDataNode>> trackLeafs,
                                 TrackKeyTID track,
                                 Rectangle MBR,
                                 TrackPoint point,
                                 TrackMessage message) {
        int i = 0;
        List<GDataNode> changeTopKLeaf = new ArrayList<>();
        for (Iterator<GDataNode> ite = track.passP.iterator(); ite.hasNext();) {
            GDataNode oldLeaf = ite.next();
            if (!allLeafs.contains(oldLeaf)){ //该pass节点发生了调整
                GDataNode newLeaf = Constants.removeElem(trackLeafs.f0, leaf -> leaf.leafID == oldLeaf.leafID);
                if (newLeaf == null){ //调整后轨迹track不经过这个leafID了
                    newLeaf = Constants.removeElem(trackLeafs.f1, leaf -> leaf.leafID == oldLeaf.leafID);
                    if (newLeaf == null){ //调整后轨迹track与这个leafID无关了
                        out.collect(new TwoThreeTrackInfo(oldLeaf.leafID, (byte) 8, point));
                    }else { //这个leafID在调整后成为轨迹track的topK节点了
                        changeTopKLeaf.add(newLeaf);
                        out.collect(new TwoThreeTrackInfo(oldLeaf.leafID, (byte) 6, point));
                    }
                    i--;
                    ite.remove();
                }else { //调整后轨迹track还经过这个leafID了
                    track.passP.set(i, newLeaf);
                }
            }
            i++;
        }
        for (Iterator<GLeafAndBound> ite = track.topKP.getList().iterator(); ite.hasNext();) {
            GDataNode oldLeaf = ite.next().leaf;
            if (!allLeafs.contains(oldLeaf)){ //该topK节点发生了调整
                ite.remove();
                GDataNode newLeaf = Constants.removeElem(trackLeafs.f1, leaf -> leaf.leafID == oldLeaf.leafID);
                if (newLeaf == null){ //调整后这个leafID不是轨迹track的topK节点
                    newLeaf = Constants.removeElem(trackLeafs.f0, leaf -> leaf.leafID == oldLeaf.leafID);
                    if (newLeaf == null){ //调整后这个leafID与轨迹track无关了
                        out.collect(new TwoThreeTrackInfo(oldLeaf.leafID, (byte) 9, point));
                    }else { //调整后这个leafID成为轨迹track的pass节点
                        track.passP.add(newLeaf);
                        out.collect(new TwoThreeTrackInfo(oldLeaf.leafID, (byte) 7, point));
                    }
                }else { //调整后这个leafID还是轨迹track的topK节点
                    changeTopKLeaf.add(newLeaf);
                }
            }
        }
        trackLeafs.f0.forEach(dataNode -> out.collect(new TwoThreeTrackInfo(dataNode.leafID, (byte) 10, message)));
        track.passP.addAll(trackLeafs.f0);
        trackLeafs.f1.forEach(leaf -> out.collect(new TwoThreeTrackInfo(leaf.leafID, (byte) 11, message)));
        DTConstants.addTrackTopK(track, MBR, changeTopKLeaf);
        DTConstants.addTrackTopK(track, MBR, trackLeafs.f1);
    }

    private void bothHasNoTopKNode(Collection<GDataNode> allLeafs,
                                   Tuple2<List<GDataNode>, List<GDataNode>> trackLeafs,
                                   TrackKeyTID track,
                                   TrackPoint point,
                                   TrackMessage message) {
        int i = 0;
        for (Iterator<GDataNode> ite = track.passP.iterator(); ite.hasNext();) {
            GDataNode oldLeaf = ite.next();
            if (!allLeafs.contains(oldLeaf)){ //该叶节点发生了调整
                GDataNode newLeaf = Constants.removeElem(trackLeafs.f0, leaf -> leaf.leafID == oldLeaf.leafID);
                if (newLeaf == null){ //调整后轨迹track不经过这个leafID了
                    out.collect(new TwoThreeTrackInfo(oldLeaf.leafID, (byte) 8, point));
                    i--;
                    ite.remove();
                }else { //调整后轨迹track还经过这个leafID了
                    track.passP.set(i, newLeaf);
                }
            }
            i++;
        }
        trackLeafs.f0.forEach(dataNode -> out.collect(new TwoThreeTrackInfo(dataNode.leafID, (byte) 10, message)));
        track.passP.addAll(trackLeafs.f0);
    }


    private TrackMessage toTrackMessage(Trajectory<Segment> track){
        List<TrackPoint> list = new ArrayList<>();
        list.add(track.elems.getFirst().p1);
        for (Segment elem : track.elems)
            list.add(elem.p2);
        return new TrackMessage(list, curWater-1);
    }


    /**
     * 窗口滑动逻辑
     * @param TIDs 滑出的tuple
     * @param startWindow 新窗口的开始时间
     */
    private void windowSlide(RoaringBitmap TIDs, long startWindow) {
        //整条轨迹滑出窗口的轨迹ID集
        Set<Integer> emptyTIDs = new HashSet<>();

        //有采样点滑出窗口的轨迹（但没有整条滑出），记录其ID和滑出的Segment
        Map<Integer, List<Segment>> removeElemMap = new HashMap<>();

        DTConstants.removeSegment(TIDs, startWindow, removeElemMap, emptyTIDs, pointIndex, trackMap);

        //记录无采样点滑出，但其topK结果可能发生变化的轨迹TID
        Set<Integer> pruneChangeTIDs = new HashSet<>();

        //记录轨迹滑出导致其候选轨迹集小于K的轨迹ID集
        Set<Integer> canSmallTIDs = new HashSet<>();

        //处理整条轨迹滑出窗口的轨迹
        for (Integer tid : emptyTIDs) {
            TrackKeyTID track = trackMap.remove(tid);
            track.enlargeTuple.f0.bitmap.remove(tid);
            for (GDataNode leaf : track.passP)
                leaf.bitmap.remove(tid);
            if (!track.topKP.isEmpty()){
                for (GLeafAndBound gb : track.topKP.getList())
                    gb.leaf.bitmap.remove(tid);
            }
            DTConstants.dealAllSlideOutTracks(track, TIDs, pruneChangeTIDs, emptyTIDs, canSmallTIDs, trackMap,null, pruneIndex);
        }


        //处理整条轨迹未完全滑出窗口的轨迹
        trackSlideOut(removeElemMap, pruneChangeTIDs, canSmallTIDs);

        pruneChangeTIDs.removeAll(canSmallTIDs);

        for (Integer tid : pruneChangeTIDs) {
            if (tid == 2563)
                System.out.print("");
            changeThreshold(trackMap.get(tid), -1, null);
        }
        for (Integer tid : canSmallTIDs) {
            if (tid == 2563)
                System.out.print("");
            dealCandidateSmall(removeElemMap, trackMap.get(tid), -1, null);
        }

        for (Integer tid : removeElemMap.keySet())
            mayBeAnotherTopK(trackMap.get(tid));
        check();
    }

    /**
     * 轨迹track有采样点滑出窗口，但不是全部。修改相关数据。
     * @param removeElemMap 有滑出的采样点的轨迹集合,及其滑出的采样点构成的map
     * @param pruneChangeTIDs 记录由于修改track相关数据而导致其裁剪域发生变化的轨迹。
     *                        不包括在hasSlideTrackIds中的轨迹。
     */
    private void trackSlideOut(Map<Integer, List<Segment>> removeElemMap,
                               Set<Integer> pruneChangeTIDs,
                               Set<Integer> canSmallTIDs) {
        Set<Integer> calculatedTIDs = new HashSet<>();
        Set<Integer> hasSlideTrackIds = removeElemMap.keySet();
        removeElemMap.forEach((tid, timeOutElems) -> {
            TrackKeyTID track = trackMap.get(tid);
            for (SimilarState state : track.getRelatedInfo().values()) {
                int comparedTid = Constants.getStateAnoTID(state, tid);
                if (!calculatedTIDs.contains(comparedTid)) { //track与comparedTid的距离没有计算过
                    TrackKeyTID comparedTrack = trackMap.get(comparedTid);
                    if (hasSlideTrackIds.contains(comparedTid)){ //本次滑动comparedTrack有采样点滑出
                        Constants.decrementHausdorff(track.trajectory, timeOutElems, comparedTrack.trajectory, removeElemMap.get(comparedTid), state);
                    }else { //本次滑动comparedTrack无采样点滑出
                        Constants.decrementHausdorff(track.trajectory, timeOutElems, comparedTrack.trajectory, new ArrayList<>(), state);
                        int oldIndex = comparedTrack.candidateInfo.indexOf(tid);
                        if (oldIndex != -1) {
                            comparedTrack.updateCandidateInfo(tid);
                            pruneChangeTIDs.add(comparedTid);
                        }
                    }
                }
            }
            if (!canSmallTIDs.contains(tid)) {
                track.sortCandidateInfo();
                recalculateOutPointTrackTopK(track);
            }
            calculatedTIDs.add(tid);
        });
    }

    private void recalculateAddPointTrackTopK(TrackKeyTID track,
                                              Segment segment) {
        Integer TID = track.trajectory.TID;
        if (track.enlargeTuple.f0 != track.passP.get(0))
            track.enlargeTuple.f0.bitmap.remove(TID);
        Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
        MBR = MBR.getUnionRectangle(segment.rect);
        double newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
        Rectangle pruneArea = MBR.clone().extendLength(newThreshold);
        boolean hasNotTopK = track.topKP.isEmpty();

        //往track经过的节点发送数据
        List<GDataNode> MBRLeafs = globalTree.getIntersectLeafNodes(MBR);
        TrackPoint point = segment.p2;
        track.passP.forEach(dataNode -> out.collect(new TwoThreeTrackInfo(dataNode.leafID, (byte) 0, point)));
        MBRLeafs.removeAll(track.passP);
        if (!MBRLeafs.isEmpty()){
            List<GLeafAndBound> gbList = track.topKP.getList();
            TrackMessage trackMessage = toTrackMessage(track.trajectory);
            for (GDataNode leaf : MBRLeafs) {
                GLeafAndBound gb = new GLeafAndBound(leaf, 0.0);
                if (gbList.contains(gb)) {
                    out.collect(new TwoThreeTrackInfo(leaf.leafID, (byte) 1, point));
                    out.collect(new TwoThreeTrackInfo(leaf.leafID, (byte) 12, point));
                    gbList.remove(gb);
                } else {
                    out.collect(new TwoThreeTrackInfo(leaf.leafID, (byte) 2, trackMessage));
                    leaf.bitmap.add(TID);
                }
            }
            track.passP.addAll(MBRLeafs);
        }

        if (hasNotTopK){ //没有topK扩展节点
            List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
            if (pruneAreaLeafs.size() == track.passP.size()){ //还没有topK扩展节点
                globalTree.countEnlargeBound(track, track.passP, MBR);
                track.rect = pruneArea;
                track.data = pruneArea.getCenter().data;
                track.threshold = newThreshold;
            }else { //有topK扩展节点了
                pruneArea = extendTopKP(track, MBR,-1, null);
                pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
                track.rect = pruneArea;
                track.data = pruneArea.getCenter().data;
                if (track.passP.size() == pruneAreaLeafs.size()) { //还没有topK扩展节点
                    track.cutOffCandidate(trackMap);
                    globalTree.countEnlargeBound(track, track.passP, MBR);
                }else { //有topK扩展节点了
                    pruneIndex.insert(track);
                    globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                    TrackMessage trackMessage = toTrackMessage(track.trajectory);
                    outAddTopKTrackGB(track.topKP.getList(), trackMessage);
                }
            }
        }else { //有topK扩展节点
            pruneArea = DTConstants.recalculateTrackTopK(track, MBR, pointIndex, trackMap);
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
                Set<GDataNode> pruneAreaLeafsSet = new HashSet<>(pruneAreaLeafs);
                pruneAreaLeafsSet.removeAll(track.passP);
                List<GDataNode> deleteLeafs = new ArrayList<>(); //记录那些原有的topK节点，添加点后不是topK节点了
                List<GDataNode> unionLeafs = new ArrayList<>(); //记录那些添加点前后都是topK节点的节点
                for (GLeafAndBound gb : track.topKP.getList()){
                    if (pruneAreaLeafsSet.contains(gb.leaf)) {
                        unionLeafs.add(gb.leaf);
                    }else {
                        deleteLeafs.add(gb.leaf);
                    }
                }
                outDeleteTopKPTrackGD(TID, deleteLeafs);
                pruneAreaLeafsSet.removeAll(unionLeafs);
                TrackMessage trackMessage = toTrackMessage(track.trajectory);
                outAddTopKTrackGD(pruneAreaLeafsSet, trackMessage);
                unionLeafs.forEach(leaf -> out.collect(new TwoThreeTrackInfo(leaf.leafID, (byte) 1, point)));
                globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
            }
        }
        track.enlargeTuple.f0.bitmap.add(TID);
    }

    /**
     * 已有的轨迹有新的采样点上传或者有采样点过时，重新计算缓存中的相似度中间结果后调用该方法。
     * 进行再一次的topK结果计算。
     */
    private void recalculateOutPointTrackTopK(TrackKeyTID track) {
        try {
            Integer TID = track.trajectory.TID;
            track.enlargeTuple.f0.bitmap.remove(TID);
            Rectangle MBR = Constants.getPruningRegion(track.trajectory, 0.0);
            double newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
            Rectangle pruneArea = MBR.clone().extendLength(newThreshold);
            List<GDataNode> MBRLeafs = outDeletePassTrack(track, MBR);
            track.passP.removeAll(MBRLeafs);
            outDeletePassTrack(TID, track.passP);
            track.passP = MBRLeafs;
            if (track.topKP.isEmpty()){ //没有topK扩展节点
                List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
                if (MBRLeafs.size() == pruneAreaLeafs.size()){ //还没有topK扩展节点
                    globalTree.countEnlargeBound(track, track.passP, MBR);
                    track.rect = pruneArea;
                    track.data = pruneArea.getCenter().data;
                    track.threshold = newThreshold;
                }else { //有topK扩展节点了
                    pruneArea = extendTopKP(track,MBR, -1, null);
                    pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
                    track.rect = pruneArea;
                    track.data = pruneArea.getCenter().data;
                    if (MBRLeafs.size() == pruneAreaLeafs.size()) { //还没有topK扩展节点
                        track.cutOffCandidate(trackMap);
                        globalTree.countEnlargeBound(track, track.passP, MBR);
                    }else { //有topK扩展节点了
                        pruneIndex.insert(track);
                        globalTree.countTopKAndEnlargeBound(track, MBRLeafs, pruneAreaLeafs, MBR);
                        TrackMessage trackMessage = toTrackMessage(track.trajectory);
                        outAddTopKTrackGB(track.topKP.getList(), trackMessage);
                    }
                }
            }else { //有topK扩展节点
                pruneArea = DTConstants.recalculateTrackTopK(track, MBR, pointIndex, trackMap);
                List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
                if (pruneAreaLeafs.size() == track.passP.size()){ //没有topK扩展节点
                    track.cutOffCandidate(trackMap);
                    outDeleteTopKPTrackGB(TID, track.topKP.getList());
                    pruneIndexRemove(track);
                    track.rect = pruneArea;
                    track.data = pruneArea.getCenter().data;
                    globalTree.countEnlargeBound( track, MBRLeafs, MBR);
                }else { //有topK扩展节点
                    pruneIndex.alterELem(track, pruneArea);
                    Set<GDataNode> pruneAreaLeafsSet = new HashSet<>(pruneAreaLeafs);
                    List<GDataNode> deleteLeafs = new ArrayList<>();
                    List<GDataNode> unionLeafs = new ArrayList<>();
                    for (GLeafAndBound gb : track.topKP.getList()){
                        if (pruneAreaLeafsSet.contains(gb.leaf))
                            unionLeafs.add(gb.leaf);
                        else
                            deleteLeafs.add(gb.leaf);
                    }
                    outDeleteTopKPTrackGD(TID, deleteLeafs);
                    pruneAreaLeafsSet.removeAll(unionLeafs);
                    TrackMessage trackMessage = toTrackMessage(track.trajectory);
                    outAddTopKTrackGD(pruneAreaLeafsSet, trackMessage);
                    globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                }
            }
            track.enlargeTuple.f0.bitmap.add(TID);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @NotNull
    private List<GDataNode> outDeletePassTrack(TrackKeyTID track, Rectangle MBR) {
        List<GDataNode> MBRLeafs = globalTree.getIntersectLeafNodes(MBR);
        if (MBRLeafs.size() < track.passP.size()){
            track.passP.removeAll(MBRLeafs);
            outDeletePassTrack(track.trajectory.TID, track.passP);
            track.passP = MBRLeafs;
        }
        return MBRLeafs;
    }

    private Rectangle extendTopKP(TrackKeyTID track, Rectangle MBR, int notRemove, List<Integer> removeRI) {
        Integer TID = track.trajectory.TID;
        SimilarState thresholdState = track.getKCanDistance(Constants.topK + Constants.t);
        double threshold = thresholdState.distance;
        Rectangle pruneArea = MBR.clone().extendLength(threshold);
        //用裁剪域筛选出候选轨迹集，计算距离并排序
        Set<Integer> needCompareTIDS = pointIndex.getRegionInternalTIDs(pruneArea);
        needCompareTIDS.remove(TID);
        List<SimilarState> needCompareState = new ArrayList<>();
        for (Integer compareTid : needCompareTIDS) {
            SimilarState state = track.getSimilarState(compareTid);
            if (state == null)
                state = Constants.getHausdorff(track.trajectory, trackMap.get(compareTid).trajectory);
            needCompareState.add(state);
        }
        Collections.sort(needCompareState);

        //删除track中原有的无关的RelatedInfo
        for (Iterator<Map.Entry<SimilarState, SimilarState>> ite = track.getRelatedInfo().entrySet().iterator(); ite.hasNext();){
            SimilarState state = ite.next().getKey();
            if (!needCompareState.contains(state)){
                int comparedTID = Constants.getStateAnoTID(state, TID);
                TrackKeyTID comparedTrack = trackMap.get(comparedTID);
                if (!comparedTrack.candidateInfo.contains(TID)) {
                    ite.remove();
                    if (comparedTID == notRemove)
                        removeRI.add(TID);
                    else
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
                        if (comparedTID == notRemove)
                            removeRI.add(TID);
                        else
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

    private void dealCandidateSmall(Map<Integer, List<Segment>> removeElemMap, TrackKeyTID track, int notRemove, List<Integer> removeRI) {
        Integer TID = track.trajectory.TID;
        if (track.enlargeTuple.f0 != track.passP.get(0))
            track.enlargeTuple.f0.bitmap.remove(TID);
        List<SimilarState> list = new ArrayList<>(track.getRelatedInfo().keySet());
        Collections.sort(list);
        List<Integer> newCanDi = new ArrayList<>();
        int i = 0;
        while (i < Constants.topK + Constants.t && i < list.size()) {
            SimilarState s = list.get(i);
            int id = Constants.getStateAnoTID(s, TID);
            newCanDi.add(id);
            i++;
        }
        track.candidateInfo.removeAll(newCanDi);
        while (!track.candidateInfo.isEmpty())
            track.removeICandidate(1, trackMap);
        track.candidateInfo = newCanDi;

        if (newCanDi.size() < Constants.topK + Constants.t) {
            Rectangle MBR;
            if (removeElemMap.get(TID) == null)
                MBR = track.rect.clone().extendLength(-track.threshold);
            else
                MBR = Constants.getPruningRegion(track.trajectory, 0.0);
            TrackKeyTID tmpTrack = new TrackKeyTID(null, null, null, track.trajectory.elems, TID, new ArrayList<>(), new HashMap<>());
            Rectangle pruneArea = MBR.clone().extendLength(track.getKCanDistance(Constants.topK).distance);
            tmpTrack.rect = DTConstants.newTrackCalculate(tmpTrack, MBR, pruneArea, pointIndex, trackMap);
            Set<SimilarState> stateSet = tmpTrack.getRelatedInfo().keySet();
            for (SimilarState state : track.getRelatedInfo().values()) {
                if (!stateSet.contains(state)) {
                    int comparedTID = Constants.getStateAnoTID(state, TID);
                    TrackKeyTID comparedTrack = trackMap.get(comparedTID);
                    if (comparedTrack.candidateInfo.contains(TID)) {
                        tmpTrack.putRelatedInfo(state);
                    }else {
                        if (comparedTID == notRemove)
                            removeRI.add(TID);
                        else
                            comparedTrack.removeRelatedInfo(state);
                    }
                }
            }
            track.setRelatedInfo(tmpTrack.getRelatedInfo());
            track.candidateInfo = tmpTrack.candidateInfo;
//            if (track.candidateInfo .contains(notRemove))
//                removeRI.remove(TID);
            track.threshold = tmpTrack.threshold;

            List<GDataNode> removePass = new ArrayList<>();
            if (removeElemMap.get(TID) != null) {
                for (Iterator<GDataNode> ite = track.passP.iterator(); ite.hasNext();){
                    GDataNode leaf = ite.next();
                    if (!leaf.rectangle.isIntersection(MBR)){
                        removePass.add(leaf);
                        ite.remove();
                    }
                }
            }

            List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(tmpTrack.rect);
            if (track.topKP.isEmpty()){ //无topK扩展节点
                track.rect = tmpTrack.rect;
                track.data = tmpTrack.rect.getCenter().data;
                if (pruneAreaLeafs.size() == track.passP.size()){ //还无topK扩展节点
                    track.cutOffCandidate(trackMap, notRemove, removeRI);
                    if (removeElemMap.get(TID) != null)
                        globalTree.countEnlargeBound(track, pruneAreaLeafs, MBR);
                }else { //有topK扩展节点了
                    pruneIndex.insert(track);
                    globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                    TrackMessage trackMessage = toTrackMessage(track.trajectory);
                    track.topKP.getList().forEach(gB -> {
                        if (removePass.remove(gB.leaf)){
                            TrackPoint point = new TrackPoint(null, curWater-1, TID);
                            out.collect(new TwoThreeTrackInfo(gB.leaf.leafID, (byte) 13, point));
                        }else {
                            out.collect(new TwoThreeTrackInfo(gB.leaf.leafID, (byte) 3, trackMessage));
                            gB.leaf.bitmap.add(TID);
                        }
                    });
                }
            }else { //有topK扩展节点
                if (pruneAreaLeafs.size() == track.passP.size()){ //无topK扩展节点
                    track.cutOffCandidate(trackMap, notRemove, removeRI);
                    outDeleteTopKPTrackGB(TID, track.topKP.getList());
                    pruneIndexRemove(track);
                    track.rect = tmpTrack.rect;
                    track.data = tmpTrack.rect.getCenter().data;
                    if (removeElemMap.get(TID) != null)
                        globalTree.countEnlargeBound(track, track.passP, MBR);
                }else { //有topK扩展节点
                    pruneIndex.alterELem(track, tmpTrack.rect);
                    Set<GDataNode> pruneAreaLeafsSet = new HashSet<>(pruneAreaLeafs);
                    pruneAreaLeafsSet.removeAll(track.passP);
                    TrackPoint point = new TrackPoint(null, curWater-1, TID);
                    TrackMessage trackMessage = toTrackMessage(track.trajectory);
                    GLeafAndBound gb = new GLeafAndBound();
                    List<GLeafAndBound> oldTopKs = track.topKP.getList();
                    List<GLeafAndBound> oriTopKs = new ArrayList<>();
                    for (Iterator<GDataNode> ite = pruneAreaLeafsSet.iterator(); ite.hasNext();) {
                        GDataNode leaf = ite.next();
                        gb.leaf = leaf;
                        int index = oldTopKs.indexOf(gb);
                        if (index != -1) { //oldTopKs中有leaf
                            oriTopKs.add(oldTopKs.remove(index));
                            ite.remove();
                            if (removePass.contains(leaf)) {
                                out.collect(new TwoThreeTrackInfo(leaf.leafID, (byte) 13, point));
                            }
                        }else { //oldTopKs中没有leaf
                            if (removePass.contains(leaf)) {
                                out.collect(new TwoThreeTrackInfo(leaf.leafID, (byte) 13, point));
                            } else {
                                out.collect(new TwoThreeTrackInfo(leaf.leafID, (byte) 3, trackMessage));
                                leaf.bitmap.add(TID);
                            }
                        }
                    }
                    outDeleteTopKPTrackGB(TID, oldTopKs);
                    if (removeElemMap.get(TID) == null){
                        if (track.threshold > track.enlargeTuple.f1 )
                            globalTree.countEnlargeBound(track, pruneAreaLeafs, MBR);
                        for (GDataNode leaf : pruneAreaLeafsSet)
                            oriTopKs.add(new GLeafAndBound(leaf, Constants.countEnlargeBound(MBR, leaf.rectangle)));
                        track.topKP.setList(oriTopKs);
                    }else {
                        globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                    }
                }
            }
            outDeletePassTrack(TID, removePass);
        }else {
            if (removeElemMap.get(TID) == null)
                changeThreshold(track, notRemove, removeRI);
            else
                recalculateOutPointTrackTopK(track);
        }
        track.enlargeTuple.f0.bitmap.add(TID);
    }


    private void processPoint(List<TrackPoint> points) {
        for (TrackPoint point : points) {
            TIDs.add(point.TID);
            Tuple2<Segment, TrackKeyTID> tuple2 = getTrajectory(point);
            if (tuple2 == null)
                continue;
            TrackKeyTID track = tuple2.f1;
            Segment segment = tuple2.f0;
            if (hasInit){ //正常计算
                if (count == 23500)
                    System.out.print("");
                if (track.trajectory.elems.size() == 1) { //新的轨迹
                    Rectangle MBR = track.rect.clone();
                    Rectangle pruneArea = MBR.clone().extendLength(Constants.extend);
                    track.rect = DTConstants.newTrackCalculate(track, MBR, pruneArea, pointIndex, trackMap);
                    globalTree.countPartitions(MBR, track);
                    track.enlargeTuple.f0.bitmap.add(track.trajectory.TID);
                    TrackMessage trackMessage = toTrackMessage(track.trajectory);
                    if (!track.topKP.isEmpty()) {
                        outAddTopKTrackGB(track.topKP.getList(), trackMessage);
                        pruneIndex.insert(track);
                    }
                    outAddPassTrack(track.passP, trackMessage);
                    mayBeAnotherTopK(track);
                }else {  //已有轨迹
                    updateTrackRelated(segment, track);
                    //重新做一次裁剪和距离计算
                    track.sortCandidateInfo();
                    recalculateAddPointTrackTopK(track, segment);
                }
                if (count%5000 == 0) {
                    System.out.println(count);
                    check();
                }
//                if (count >= 8493)
//                    check();
                count++;
            }else { //初始计算
                if (track.trajectory.elems.size() != 1) {//已有轨迹
                    track.rect = track.rect.getUnionRectangle(segment.rect);
                    track.data = track.rect.getCenter().data;
                }
                globalTree.root.getIntersectLeafNodes(track.rect, track.passP);
                for (GDataNode leaf : track.passP)
                    leaf.bitmap.add(track.trajectory.TID);
                boolean canInit = true;
                for (GDataNode leaf : globalTree.leafIDMap.values()) {
                    if (leaf.bitmap.toArray().length < Constants.topK + 1){
                        canInit = false;
                        break;
                    }
                }
                if (canInit){
                    hasInit = true;
                    initCalculate();
                    check();
                }
            }
            
        }
    }

    private void pruneIndexRemove(TrackKeyTID track){
        pruneIndex.delete(track);
        GLeafAndBound gB = track.topKP.getFirst();
        track.enlargeTuple.f0 = gB.leaf;
        track.enlargeTuple.f1 = gB.bound;
        track.topKP.clear();
    }



    /**
     * 已有轨迹track有新的轨迹段seg到达，更新缓存中其相关的中间结果
     */
    private void updateTrackRelated(Segment seg, TrackKeyTID track) {
        List<Integer> removeRI = new ArrayList<>();
        Integer TID = track.trajectory.TID;
        //更新新采样点所属的轨迹与相关轨迹的距离
        for (SimilarState state : track.getRelatedInfo().values()) {
            int comparedTID = Constants.getStateAnoTID(state, TID);
            TrackKeyTID comparedTrack = trackMap.get(comparedTID);
            if (count == 8493 && comparedTID == 34)
                System.out.print("");
            int oldIndex = comparedTrack.candidateInfo.indexOf(TID);
            Constants.incrementHausdorff(Collections.singletonList(seg.p2), comparedTrack.trajectory, state);
            if (oldIndex != -1){ //track是comparedTrack的候选轨迹
                if (comparedTrack.rect.isInternal(seg.p2)){
                    comparedTrack.updateCandidateInfo(TID);
                    changeThreshold(comparedTrack, TID, removeRI);
                }else {
                    comparedTrack.candidateInfo.remove(oldIndex);
                    if (comparedTrack.candidateInfo.size() < Constants.topK){
                        dealCandidateSmall(new HashMap<>(), comparedTrack, TID, removeRI);
                    }else {
                        changeThreshold(comparedTrack, TID, removeRI);
                    }
                    if (!track.candidateInfo.contains(comparedTID) &&
                            !comparedTrack.candidateInfo.contains(TID)){
                        comparedTrack.removeRelatedInfo(TID);
                        removeRI.add(comparedTID);
                    }
                }
            }
        }
        for (Integer compareTid : removeRI)
            track.removeRelatedInfo(compareTid);
    }

    /**
     * 轨迹的阈值可能发生变化时，调用该函数，进行修改阈值的一系列操作
     * @param notRemove 修改阈值时，可能删除缓存中的某个中间状态。但notRemoveTid状态不能现在删除，将其添加到removeRI，后续再删除
     */
    private void changeThreshold(TrackKeyTID track, int notRemove, List<Integer> removeRI) {
        double dis = track.getKCanDistance(Constants.topK).distance;
        Integer TID = track.trajectory.TID;
        if (dis > track.threshold) { //需扩大阈值
            dis = track.getKCanDistance(Constants.topK + Constants.t).distance;
            Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
            if (track.topKP.isEmpty()) { //无topK扩展节点
                if (dis > track.enlargeTuple.f1) { //无topK扩展节点，现在需要扩展
                    track.rect = extendTopKP(track, MBR,notRemove, removeRI);
                    List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(track.rect);
                    if (track.passP.get(0) != track.enlargeTuple.f0)
                        track.enlargeTuple.f0.bitmap.remove(TID);
                    if (track.passP.size() == pruneAreaLeafs.size()) { //还没有topK扩展节点
                        track.cutOffCandidate(trackMap, notRemove, removeRI);
                        globalTree.countEnlargeBound(track, track.passP, MBR);
                    }else { //有topK扩展节点了
                        pruneIndex.insert(track);
                        globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                        TrackMessage trackMessage = toTrackMessage(track.trajectory);
                        outAddTopKTrackGB(track.topKP.getList(), trackMessage);
                    }
                    track.enlargeTuple.f0.bitmap.add(TID);
                }else {  //无topK扩展节点，现在不需要扩展
                    track.rect.extendLength(-track.threshold + dis);
                    track.threshold = dis;
                }
            }else{  //有topK扩展节点
                if (track.passP.get(0) != track.enlargeTuple.f0)
                    track.enlargeTuple.f0.bitmap.remove(TID);
                Tuple2<Boolean, Rectangle> tuple2 = DTConstants.enlargePrune(track, dis, notRemove, pointIndex, trackMap);
                if (tuple2.f0)
                    removeRI.add(TID);
                if (track.enlargeTuple.f1 < track.threshold) { //topK扩展节点变多
                    pruneIndex.alterELem(track, tuple2.f1);
                    List<GDataNode> leafs = globalTree.enlargePartitions(track, MBR);
                    //通知扩展
                    TrackMessage trackMessage = toTrackMessage(track.trajectory);
                    outAddTopKTrackGD(leafs, trackMessage);
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
                            track.cutOffCandidate(trackMap, notRemove, removeRI);
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
                Rectangle pruneArea = DTConstants.tightenThresholdCommon(track, notRemove, removeRI, trackMap);
                if (track.threshold < track.topKP.getLast().bound){ //topK扩展节点变少
                    if (track.threshold < track.topKP.getFirst().bound){ //topK扩展节点变没
                        outDeleteTopKPTrackGB(TID, track.topKP.getList());
                        pruneIndexRemove(track);
                        track.rect = pruneArea;
                        track.cutOffCandidate(trackMap, notRemove, removeRI);
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
                if (comparedTid == 108 && count == 525)
                    System.out.print("");
                Constants.addTrackCandidate(comparedTrack, track);
                comparedTrack.updateCandidateInfo(TID);
                if (comparedTrack.getKCanDistance(Constants.topK + Constants.t*2).distance < comparedTrack.threshold) {
                    double newThreshold = comparedTrack.getKCanDistance(Constants.topK + Constants.t).distance;
                    Rectangle pruneArea = comparedTrack.rect.clone().extendLength(newThreshold - comparedTrack.threshold);
                    DTConstants.cacheTighten(comparedTrack, pruneArea, -1, trackMap);
                    comparedTrack.threshold = newThreshold;
                    List<GLeafAndBound> removeList = comparedTrack.topKP.removeBigger(new GLeafAndBound(null, newThreshold));
                    if (!removeList.isEmpty()) { //阈值变小topK节点变少了
                        comparedTrack.enlargeTuple.f0.bitmap.remove(comparedTid);
                        outDeleteTopKPTrackGB(comparedTid, removeList);
                        GLeafAndBound last = removeList.get(removeList.size()-1);
                        comparedTrack.enlargeTuple.f0 = last.leaf;
                        comparedTrack.enlargeTuple.f1 = last.bound;
                        comparedTrack.enlargeTuple.f0.bitmap.add(comparedTid);
                        if (comparedTrack.topKP.isEmpty()) { //没有topK节点了
                            pruneIndex.delete(comparedTrack);
                            comparedTrack.topKP.clear();
                            comparedTrack.rect = pruneArea;
                            comparedTrack.leaf = null;
                            comparedTrack.cutOffCandidate(trackMap);
                            continue;
                        }
                    }
                    pruneIndex.alterELem(comparedTrack, pruneArea);
                }
            }
        }
    }



    private void initCalculate(){
        for (TrackKeyTID track : trackMap.values()) {
            int TID = track.trajectory.TID;
            if (TID == 155)
                System.out.print("");
            track.passP.clear();
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
            TrackMessage trackMessage = toTrackMessage(track.trajectory);
            if (!track.topKP.isEmpty()) {
                outAddTopKTrackGB(track.topKP.getList(), trackMessage);
                pruneIndex.insert(track);
                for (int i = Constants.t + Constants.topK; i < track.candidateInfo.size(); i++) {
                    int comparedTID = track.candidateInfo.get(i);
                    TrackKeyTID comparedTrack = trackMap.get(comparedTID);
                    if (comparedTrack.outSideRectangle(track.rect)){
                        track.removeTIDCandidate(comparedTrack);
                        i--;
                    }
                }
            }
            outAddPassTrack(track.passP,trackMessage);
        }
    }

    /**
     * 通知removeList中的对应的divide节点删除topK轨迹comparedTid
     */
    private void outDeleteTopKPTrackGB(Integer TID, List<GLeafAndBound> removeList) {
        TrackPoint point = new TrackPoint(null, curWater-1, TID);
        removeList.forEach(gB -> {
            gB.leaf.bitmap.remove(TID);
            out.collect(new TwoThreeTrackInfo(gB.leaf.leafID, (byte) 5, point));
        });
    }

    /**
     * 通知removeList中的对应的divide节点删除topK轨迹comparedTid
     */
    private void outDeleteTopKPTrackGD(Integer TID, List<GDataNode> removeList) {
        TrackPoint point = new TrackPoint(null, curWater-1, TID);
        removeList.forEach(leaf -> {
            leaf.bitmap.remove(TID);
            out.collect(new TwoThreeTrackInfo(leaf.leafID, (byte) 5, point));
        });
    }

    /**
     * 通知removeList中的对应的divide节点删除topK轨迹comparedTid
     */
    private void outDeletePassTrack(int TID, List<GDataNode> leafs) {
        TrackPoint point = new TrackPoint(null, curWater-1, TID);
        leafs.forEach(leaf -> {
            leaf.bitmap.remove(TID);
            out.collect(new TwoThreeTrackInfo(leaf.leafID, (byte) 4, point));
        });
    }

    /**
     * 通知list中的对应的divide节点,添加一条新的topK轨迹trackMessage
     */
    private void outAddTopKTrackGB(List<GLeafAndBound> list, TrackMessage trackMessage) {
        int TID = trackMessage.elems.get(0).TID;
        list.forEach(gB -> {
            out.collect(new TwoThreeTrackInfo(gB.leaf.leafID, (byte) 3, trackMessage));
            gB.leaf.bitmap.add(TID);
        });
    }

    /**
     * 通知list中的对应的divide节点,添加一条新的topK轨迹trackMessage
     */
    private void outAddTopKTrackGD(Collection<GDataNode> leafs, TrackMessage trackMessage) {
        int TID = trackMessage.elems.get(0).TID;
        leafs.forEach(leaf -> {
            out.collect(new TwoThreeTrackInfo(leaf.leafID, (byte) 3, trackMessage));
            leaf.bitmap.add(TID);
        });
    }

    /**
     * 通知list中的对应的divide节点,添加一条新的路过轨迹trackMessage
     */
    private void outAddPassTrack(List<GDataNode> list, TrackMessage trackMessage) {
        int TID = trackMessage.elems.get(0).TID;
        list.forEach(dataNode -> {
            out.collect(new TwoThreeTrackInfo(dataNode.leafID, (byte) 2, trackMessage));
            dataNode.bitmap.add(TID);
        });
    }




    private List<TrackPoint> removePointQueue(long timeStamp){
        List<TrackPoint> list = new ArrayList<>();
        while (!pointQueue.isEmpty() && pointQueue.getLast().timestamp < timeStamp)
            list.add(pointQueue.removeLast());
        return list;
    }

    @Override
    public void open(Configuration parameters) {
        subTask = getRuntimeContext().getIndexOfThisSubtask();
        hasInit = false;
        globalTree = new GTree();
        curWater = 0L;
        startWindow = 0L;
        if (subTask == 0){
            globalTree.mainSubtaskInit();
            densities = new LinkedList<>();
        }
        waterMap = new HashMap<>();
        densityMap = new HashMap<>();
        pointQueue = new LinkedList<>();
        comparator = Comparator.comparingLong(o -> o.timestamp);

        trackMap = new HashMap<>();
        singlePointMap = new HashMap<>();
        pointIndex = new RCtree<>(8,1,17, Constants.globalRegion,0, true);
        pruneIndex  = new RCtree<>(8,1,17, Constants.globalRegion,0, false);
        TIDs = new RoaringBitmap();
        count = 0;
    }

    private Tuple2<Segment, TrackKeyTID> getTrajectory(TrackPoint trackPoint) {
        TrackKeyTID track = trackMap.get(trackPoint.TID);
        Segment segment;
        if (track == null){
            TrackPoint prePoint = singlePointMap.get(trackPoint.TID);
            if (prePoint == null){
                singlePointMap.put(trackPoint.TID, trackPoint);
                return null;
            }else {
                singlePointMap.remove(trackPoint.TID);
                segment = new Segment(prePoint, trackPoint);
                track = new TrackKeyTID(null,
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
        return new Tuple2<>(segment, track);
    }

}
