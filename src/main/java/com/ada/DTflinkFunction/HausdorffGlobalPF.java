package com.ada.DTflinkFunction;

import com.ada.Hausdorff.Hausdorff;
import com.ada.Hausdorff.SimilarState;
import com.ada.QBSTree.RCtree;
import com.ada.common.ArrayQueue;
import com.ada.common.Arrays;
import com.ada.common.Constants;
import com.ada.common.collections.Collections;
import com.ada.common.collections.Judge;
import com.ada.geometry.*;
import com.ada.geometry.track.TrackHauOne;
import com.ada.geometry.track.TrackKeyTID;
import com.ada.globalTree.GDataNode;
import com.ada.globalTree.GDirNode;
import com.ada.globalTree.GNode;
import com.ada.globalTree.GTree;
import com.ada.model.densityToGlobal.Density;
import com.ada.model.densityToGlobal.D2GElem;
import com.ada.model.globalToLocal.G2LCount;
import com.ada.model.globalToLocal.G2LElem;
import com.ada.model.globalToLocal.G2LPoints;
import com.ada.model.globalToLocal.G2TID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;
import redis.clients.jedis.Jedis;

import java.nio.charset.StandardCharsets;
import java.util.*;


public class HausdorffGlobalPF extends ProcessWindowFunction<D2GElem, G2LElem, Integer, TimeWindow> {

    private boolean hasInit;
    private int subTask;
    private GTree globalTree;
    private Queue<int[][]> densityQue;
    private Queue<Tuple2<Long, RoaringBitmap>> tIDsQue;
    private Jedis jedis;

    private Map<Integer, TrackKeyTID> trackMap;
    private Map<Integer, TrackPoint> singlePointMap;
    private RCtree<Segment> segmentIndex;
    private RCtree<TrackKeyTID> pruneIndex;
    private Collector<G2LElem> out;
    private int count;

    private RoaringBitmap outTIDs;
    private RoaringBitmap inTIDs;
    private RoaringBitmap inAndOutTIDs;

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<D2GElem> elements,
                        Collector<G2LElem> out) {
        this.out = out;
        globalTree.subTask = subTask;
        if (count >= 27)
            System.out.print("");
        List<TrackKeyTID> newTracks = new ArrayList<>();
        Map<Integer, List<TrackPoint>> inPointsMap = new HashMap<>();
        int[][] density = preElements(elements, newTracks, inPointsMap);
        RoaringBitmap inputIDs = new RoaringBitmap();
        inputIDs.add(inPointsMap.keySet().stream().mapToInt(Integer::valueOf).toArray());
        tIDsQue.add(new Tuple2<>(context.window().getStart(), inputIDs));

        long logicWinStart = context.window().getEnd() - Constants.windowSize*Constants.logicWindow;
        if (hasInit){
            if (tIDsQue.element().f0 < logicWinStart) {
                outTIDs = tIDsQue.remove().f1;
            }else {
                outTIDs = new RoaringBitmap();
            }
            inAndOutTIDs = inputIDs.clone();
            inTIDs = inputIDs.clone();
            inAndOutTIDs.and(outTIDs);
            inTIDs.andNot(inAndOutTIDs);
            outTIDs.andNot(inAndOutTIDs);
            Set<Integer> emptyTracks = new HashSet<>();
            DTConstants.removeSegment(outTIDs, inAndOutTIDs, logicWinStart, emptyTracks, segmentIndex, trackMap);
            for (Integer TID : emptyTracks) outTIDs.remove(TID);
            //记录无采样点滑出，但其topK结果可能发生变化的轨迹
            Set<TrackKeyTID> pruneChangeTracks = new HashSet<>();
            //记录无采样点滑出，别的轨迹滑出导致其候选轨迹集小于K的轨迹
            Set<TrackKeyTID> canSmallTracks = new HashSet<>();
            //处理整条轨迹滑出窗口的轨迹
            for (Integer TID : emptyTracks) {
                dealAllSlideOutTracks(TID, pruneChangeTracks, emptyTracks, canSmallTracks);
            }
            for (Integer tid : emptyTracks) trackMap.remove(tid);
            //处理整条轨迹未完全滑出窗口的轨迹
            processUpdatedTrack(inPointsMap, pruneChangeTracks, canSmallTracks);
            pruneChangeTracks.removeAll(canSmallTracks);
            for (TrackKeyTID canSmallTrack : canSmallTracks) pruneChangeTracks.remove(canSmallTrack);
            for (TrackKeyTID track : pruneChangeTracks) {
                if (count == 12 && track.trajectory.TID == 16604)
                    System.out.print("");
                changeThreshold(track);
            }
            for (TrackKeyTID track : canSmallTracks) dealCandidateSmall(track);
            for (Integer tid : outTIDs) mayBeAnotherTopK(trackMap.get(tid));
            for (Integer tid : inAndOutTIDs) mayBeAnotherTopK(trackMap.get(tid));
            for (TrackKeyTID track : newTracks) mayBeAnotherTopK(track);

            if (subTask == 0)
                System.out.println(count);
            if (count%10000 == 0)
                check();

            if (density != null) {
                densityQue.add(density);
                Arrays.addArrsToArrs(globalTree.density, density, true);
                Arrays.addArrsToArrs(globalTree.density, densityQue.remove(), false);
                //调整Global Index
                Map<GNode, GNode> nodeMap = globalTree.updateTree();

                testEqualByRedis(context.window().getStart());

                //Global Index发生了调整，通知Local Index迁移数据，重建索引。
                if (!nodeMap.isEmpty()){
                    if (subTask == 0) {
                        //通知Local Index其索引区域发生的变化
                        adjustLocalTasksRegion(nodeMap);
                    }
                    redispatchGNode(nodeMap);
                    if (count >= 10000)
                        check();
                }
            }
            for (TrackKeyTID track : trackMap.values()) {
                G2LPoints trackPs = G2LPoints.toG2LPoints(track.trajectory);
                for (GDataNode leaf : track.passP) {
                    out.collect(new G2LElem(leaf.leafID, (byte) 15, trackPs));
                }
                if (!track.topKP.isEmpty()) {
                    for (GLeafAndBound gb : track.topKP.getList()) {
                        out.collect(new G2LElem(gb.leaf.leafID, (byte) 16, trackPs));
                    }
                }
            }
            if (subTask == 0) {
                for (GDataNode leaf : globalTree.getAllLeafs())
                    out.collect(new G2LElem(leaf.leafID, (byte) 17, new G2LCount(count)));
            }
        }else { //轨迹足够多时，才开始计算，计算之间进行初始化
            forInitCode(density, logicWinStart);
        }
        count++;
    }

    private void testEqualByRedis(long winStart) {
        byte[] redisKey = (winStart + "|" + "globalTree").getBytes(StandardCharsets.UTF_8);
        long size = jedis.llen(redisKey);
        if (size < Constants.globalPartition-1) {
            jedis.rpush(redisKey, Arrays.toByteArray(globalTree));
        }else if (size == 3){
            List<byte[]> list = jedis.lrange(redisKey,0, -1);
            jedis.ltrim(redisKey, 1,0);
            GTree[] gTrees = new GTree[list.size()];
            for (int i = 0; i < list.size(); i++)
                gTrees[i] = (GTree) Arrays.toObject(list.get(i));
            for (GTree gTree : gTrees) {
                if (!globalTree.equals(gTree))
                    System.out.print("");
            }
        }else {
            throw new IllegalArgumentException("redisKey too large");
        }
    }

    /**
     * 轨迹集emptyElemMap的所有采样点都滑出窗口，删除相关数据。
     * @param pruneChangeTracks 记录由于删除track而导致其裁剪域发生变化的轨迹。不包括在hasSlideTrackIds中的轨迹
     */
    void dealAllSlideOutTracks(Integer TID,
                               Set<TrackKeyTID> pruneChangeTracks,
                               Set<Integer> emptyTIDs,
                               Set<TrackKeyTID> canSmallTracks) {
        TrackKeyTID track = trackMap.get(TID);
        track.enlargeTuple.f0.bitmap.remove(TID);
        for (GDataNode leaf : track.passP) leaf.bitmap.remove(TID);
        if (!track.topKP.isEmpty()){
            for (GLeafAndBound gb : track.topKP.getList()) gb.leaf.bitmap.remove(TID);
        }
        pruneIndex.delete(track);
        for (SimilarState state : track.getRelatedInfo().values()) {
            int comparedTid = state.getStateAnoTID(TID);
            TrackKeyTID comparedTrack = trackMap.get(comparedTid);
            if (emptyTIDs.contains(comparedTid))
                continue;
            int index = comparedTrack.candidateInfo.indexOf(TID);
            comparedTrack.removeRelatedInfo(state);
            if (index != -1) {
                comparedTrack.candidateInfo.remove(TID);
                if (!inAndOutTIDs.contains(comparedTid) && !outTIDs.contains(comparedTid) && !inTIDs.contains(comparedTid)){
                    pruneChangeTracks.add(comparedTrack);
                    //comparedTrack的候选轨迹集太少了
                    if (comparedTrack.candidateInfo.size() < Constants.topK) canSmallTracks.add(comparedTrack);
                }
            }
        }
    }

    private void adjustLocalTasksRegion(Map<GNode, GNode> nodeMap){
        List<GDataNode> oldLeafs = new ArrayList<>();
        List<GDataNode> newLeafs = new ArrayList<>();
        nodeMap.forEach((oldNode, newNode) -> {
            oldNode.getLeafs(oldLeafs);
            newNode.getLeafs(newLeafs);
        });
        for (GDataNode newLeaf : newLeafs) {
            out.collect(new G2LElem(newLeaf.leafID, (byte) 14, newLeaf.region));
        }
        for (GDataNode oldLeaf : oldLeafs) {
            if (!newLeafs.contains(oldLeaf)){
                out.collect(new G2LElem(oldLeaf.leafID, (byte) 14, null));
            }
        }

    }

    private void redispatchGNode(Map<GNode, GNode> map) {
        //记录轨迹的enlarge node bound在在修改节点集中的轨迹
        RoaringBitmap enlargeTIDs = new RoaringBitmap();
        for (GNode oldNode : map.keySet()) {
            List<GDataNode> oldLeafs = new ArrayList<>();
            oldNode.getLeafs(oldLeafs);
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
        }

        Set<GDataNode> delLeaves = new HashSet<>();
        Set<GDataNode> newLeaves = new HashSet<>();
        //记录轨迹在调整后的节点newNode中的经过节点和topK节点
        //key：轨迹ID
        //value：该轨迹需要出现的节点{tuple0:为经过节点，tuple1为topK节点}
        Map<Integer, Tuple2<List<GDataNode>,List<GDataNode>>> trackLeafsMap = new HashMap<>();
        map.forEach((oldNode, newNode) -> {
            Map<Integer, Tuple2<List<GDataNode>,List<GDataNode>>> trackLeafsInnerMap = new HashMap<>();
            List<GDataNode> oldLeafs = new ArrayList<>();
            oldNode.getLeafs(oldLeafs);
            List<GDataNode> newLeafs = new ArrayList<>();
            newNode.getLeafs(newLeafs);
            delLeaves.addAll(oldLeafs);
            newLeaves.addAll(newLeafs);
            for (GDataNode oldLeaf : oldLeafs) {
                for (Integer TID : oldLeaf.bitmap) {
                    TrackKeyTID track = trackMap.get(TID);
                    //轨迹track在newNode子树上的经过叶节点和topK叶节点
                    trackLeafsInnerMap.computeIfAbsent(TID, key -> {
                        Tuple2<List<GDataNode>,List<GDataNode>> value = new Tuple2<>(new ArrayList<>(), new ArrayList<>());
                        Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
                        newNode.getIntersectLeafNodes(MBR, value.f0);
                        newNode.getIntersectLeafNodes(track.rect, value.f1);
                        value.f1.forEach(leaf -> leaf.bitmap.add(TID));
                        if (!enlargeTIDs.contains(TID)) {
                            Set<GDataNode> newLeafsSet = new HashSet<>(newLeafs);
                            for (GDataNode dataNode : value.f1) newLeafsSet.remove(dataNode);
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
        for (GDataNode leaf : newLeaves)
            delLeaves.remove(leaf);
        trackLeafsMap.forEach((TID, trackLeafs) -> {
            TrackKeyTID track = trackMap.get(TID);
            if (count == 16 && TID == 6146)
                System.out.print("");
            Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
            G2LPoints trackPs = G2LPoints.toG2LPoints(track.trajectory);
            if (track.topKP.isEmpty() && trackLeafs.f1.isEmpty()){ //修改前后都没有topK节点
                bothHasNoTopKNode(newLeaves, delLeaves, trackLeafs, track, MBR, trackPs);
            }else if(track.topKP.isEmpty() && !trackLeafs.f1.isEmpty()) { //调整后track有了topK节点
                topKAppear(newLeaves, delLeaves, trackLeafs, track, MBR, trackPs);
            }else if (!track.topKP.isEmpty() && !trackLeafs.f1.isEmpty()){ //修改前后都有topK节点
                bothHasTopKNode(newLeaves, delLeaves, trackLeafs, track, MBR, trackPs);
            }else { //调整后track没了topK节点
                topKDisappear(newLeaves, delLeaves, trackLeafs, track, MBR, trackPs);
            }
        });

        RoaringBitmap rb = new RoaringBitmap();
        rb.add(trackLeafsMap.keySet().stream().mapToInt(Integer::valueOf).toArray());
        enlargeTIDs.andNot(rb);
        for (Integer TID : enlargeTIDs) {
            TrackKeyTID track = trackMap.get(TID);
            if (count == 43 && TID == 20485)
                System.out.print("");
            List<GDataNode> removeLeafs = new ArrayList<>(track.passP);
            if (!track.topKP.isEmpty()){
                for (GLeafAndBound gb : track.topKP.getList())
                    removeLeafs.add(gb.leaf);
            }
            globalTree.countEnlargeBound(track, removeLeafs, track.rect.clone().extendLength(-track.threshold));
            track.enlargeTuple.f0.bitmap.add(TID);
        }
    }

    private void bothHasNoTopKNode(Set<GDataNode> newLeaves,
                                   Set<GDataNode> delLeaves,
                                   Tuple2<List<GDataNode>, List<GDataNode>> trackLeafs,
                                   TrackKeyTID track,
                                   Rectangle MBR,
                                   G2LPoints trackPs) {
        int TID = track.trajectory.TID;
        if (track.enlargeTuple.f0 != track.passP.get(0)) track.enlargeTuple.f0.bitmap.remove(TID);
        int i = 0;
        for (Iterator<GDataNode> ite = track.passP.iterator(); ite.hasNext();) {
            GDataNode passLeaf = ite.next();
            if (delLeaves.contains(passLeaf)){ //调整后，该节点被废弃
                i--;
                ite.remove();
            }else { //调整后存在该节点
                if (newLeaves.contains(passLeaf)){ //该节点在调整节点中
                    GDataNode newLeaf = Collections.removeAndGatherElem(trackLeafs.f0, leaf -> leaf.leafID == passLeaf.leafID);
                    if (newLeaf == null){ //调整后轨迹track不经过这个leafID了
                        out.collect(new G2LElem(passLeaf.leafID, (byte) 10, new G2TID(TID)));
                        i--;
                        ite.remove();
                    }else { //调整后轨迹track还经过这个leafID了
                        track.passP.set(i, newLeaf);
                    }
                }
            }
            i++;
        }
        trackLeafs.f0.forEach(dataNode -> out.collect(new G2LElem(dataNode.leafID, (byte) 12, trackPs)));
        track.passP.addAll(trackLeafs.f0);
        globalTree.countEnlargeBound(track, track.passP, MBR);
        track.enlargeTuple.f0.bitmap.add(TID);
    }

    private void bothHasTopKNode(Set<GDataNode> newLeaves,
                                 Set<GDataNode> delLeaves,
                                 Tuple2<List<GDataNode>, List<GDataNode>> trackLeafs,
                                 TrackKeyTID track,
                                 Rectangle MBR,
                                 G2LPoints trackPs) {
        int i = 0;
        List<GDataNode> changeTopKLeaf = new ArrayList<>();
        int TID = track.trajectory.TID;
        G2TID glTID = new G2TID(TID);
        if (track.enlargeTuple.f0 != track.passP.get(0)) track.enlargeTuple.f0.bitmap.remove(TID);
        for (Iterator<GDataNode> ite = track.passP.iterator(); ite.hasNext();) {
            GDataNode passLeaf = ite.next();
            if (delLeaves.contains(passLeaf)){ //调整后，该节点被废弃
                i--;
                ite.remove();
            }else { //调整后存在该节点
                if (newLeaves.contains(passLeaf)) { //该节点在调整节点中
                    GDataNode newLeaf = Collections.removeAndGatherElem(trackLeafs.f0, leaf -> leaf.leafID == passLeaf.leafID);
                    if (newLeaf == null) { //调整后轨迹track不经过这个leafID了
                        newLeaf = Collections.removeAndGatherElem(trackLeafs.f1, leaf -> leaf.leafID == passLeaf.leafID);
                        if (newLeaf == null) { //调整后轨迹track与这个leafID无关了
                            out.collect(new G2LElem(passLeaf.leafID, (byte) 10, glTID));
                        } else { //这个leafID在调整后成为轨迹track的topK节点了
                            changeTopKLeaf.add(newLeaf);
                            out.collect(new G2LElem(passLeaf.leafID, (byte) 8, glTID));
                        }
                        i--;
                        ite.remove();
                    } else { //调整后轨迹track还经过这个leafID了
                        track.passP.set(i, newLeaf);
                    }
                }
            }
            i++;
        }
        for (Iterator<GLeafAndBound> ite = track.topKP.getList().iterator(); ite.hasNext();) {
            GDataNode topKLeaf = ite.next().leaf;
            if (delLeaves.contains(topKLeaf)){ //调整后，该节点被废弃
                ite.remove();
            }else {//调整后存在该节点
                if (newLeaves.contains(topKLeaf)) { //该节点在调整节点中
                    GDataNode newLeaf = Collections.removeAndGatherElem(trackLeafs.f1, leaf -> leaf.leafID == topKLeaf.leafID);
                    if (newLeaf == null) { //调整后这个leafID不是轨迹track的topK节点
                        newLeaf = Collections.removeAndGatherElem(trackLeafs.f0, leaf -> leaf.leafID == topKLeaf.leafID);
                        if (newLeaf == null) { //调整后这个leafID与轨迹track无关了
                            out.collect(new G2LElem(topKLeaf.leafID, (byte) 11, glTID));
                        } else { //调整后这个leafID成为轨迹track的pass节点
                            track.passP.add(newLeaf);
                            out.collect(new G2LElem(topKLeaf.leafID, (byte) 9, glTID));
                        }
                    } else { //调整后这个leafID还是轨迹track的topK节点
                        changeTopKLeaf.add(newLeaf);
                    }
                    ite.remove();
                }
            }
        }
        trackLeafs.f0.forEach(dataNode -> out.collect(new G2LElem(dataNode.leafID, (byte) 12, trackPs)));
        track.passP.addAll(trackLeafs.f0);
        trackLeafs.f1.forEach(leaf -> out.collect(new G2LElem(leaf.leafID, (byte) 13, trackPs)));
        changeTopKLeaf.addAll(trackLeafs.f1);
        changeTopKLeaf.addAll(track.passP);
        changeTopKLeaf.addAll(Collections.changeCollectionElem(track.topKP.getList(), gb -> gb.leaf));
        globalTree.countTopKAndEnlargeBound(track, track.passP, changeTopKLeaf,MBR);
        track.enlargeTuple.f0.bitmap.add(TID);
    }

    private void topKAppear(Set<GDataNode> newLeaves,
                            Set<GDataNode> delLeaves,
                            Tuple2<List<GDataNode>, List<GDataNode>> trackLeafs,
                            TrackKeyTID track,
                            Rectangle MBR,
                            G2LPoints trackPs) {
        int TID = track.trajectory.TID;
        G2TID glTID = new G2TID(TID);
        if (track.enlargeTuple.f0 != track.passP.get(0)) track.enlargeTuple.f0.bitmap.remove(TID);
        double oldThreshold = track.threshold;
        track.rect = extendTopKP(track, MBR, false);
        if (oldThreshold < track.threshold){
            List<GDataNode> pruneLeafs = globalTree.getIntersectLeafNodes(track.rect);
            pruneLeafs.removeAll(globalTree.getIntersectLeafNodes(MBR));
            for (GDataNode leaf : pruneLeafs) leaf.bitmap.add(TID);
            trackLeafs.f1 = pruneLeafs;
        }else {
            for(Iterator<GDataNode> ite = trackLeafs.f1.iterator(); ite.hasNext();){
                GDataNode topKLeaf = ite.next();
                if (!track.rect.isIntersection(topKLeaf.region)){
                    ite.remove();
                    topKLeaf.bitmap.remove(TID);
                }
            }
        }
        if (trackLeafs.f1.isEmpty()) { //还没有topK扩展节点
            bothHasNoTopKNode(newLeaves, delLeaves, trackLeafs, track, MBR, trackPs);
            track.cutOffCandidate(trackMap);
        }else { //有topK扩展节点了
            pruneIndex.insert(track);
            track.topKP.setList(new ArrayList<>());
            int i = 0;
            for (Iterator<GDataNode> ite = track.passP.iterator(); ite.hasNext();) {
                GDataNode oldLeaf = ite.next();
                if (delLeaves.contains(oldLeaf)){ //调整后，该节点被废弃
                    i--;
                    ite.remove();
                }else { //调整后存在该节点
                    if (newLeaves.contains(oldLeaf)) { //该节点在调整节点中
                        GDataNode newLeaf = Collections.removeAndGatherElem(trackLeafs.f0, leaf -> leaf.leafID == oldLeaf.leafID);
                        if (newLeaf == null) { //调整后轨迹track不经过这个leafID了
                            newLeaf = Collections.removeAndGatherElem(trackLeafs.f1, leaf -> leaf.leafID == oldLeaf.leafID);
                            if (newLeaf == null) { //调整后轨迹track与这个leafID无关了
                                out.collect(new G2LElem(oldLeaf.leafID, (byte) 10, glTID));
                            } else { //这个leafID在调整后成为轨迹track的topK节点了
                                double bound = Constants.countEnlargeBound(MBR, newLeaf.region);
                                track.topKP.add(new GLeafAndBound(newLeaf, bound));
                                out.collect(new G2LElem(oldLeaf.leafID, (byte) 8, glTID));
                            }
                            i--;
                            ite.remove();
                        } else { //调整后轨迹track还经过这个leafID了
                            track.passP.set(i, newLeaf);
                        }
                    }
                }
                i++;
            }
            trackLeafs.f0.forEach(dataNode -> out.collect(new G2LElem(dataNode.leafID, (byte) 12, trackPs)));
            track.passP.addAll(trackLeafs.f0);
            trackLeafs.f1.forEach(leaf -> out.collect(new G2LElem(leaf.leafID, (byte) 13, trackPs)));
            track.topKP.addAll(Collections.changeCollectionElem(trackLeafs.f1, leaf -> new GLeafAndBound(leaf, Constants.countEnlargeBound(MBR, leaf.region))));
            List<GDataNode> removeLeafs = new ArrayList<>(track.passP);
            removeLeafs.addAll(Collections.changeCollectionElem(track.topKP.getList(), gb -> gb.leaf));
            globalTree.countEnlargeBound(track, removeLeafs, MBR);
            track.enlargeTuple.f0.bitmap.add(TID);
        }
    }

    private void topKDisappear(Set<GDataNode> newLeaves,
                               Set<GDataNode> delLeaves,
                               Tuple2<List<GDataNode>, List<GDataNode>> trackLeafs,
                               TrackKeyTID track,
                               Rectangle MBR,
                               G2LPoints trackPs) {
        int i = 0;
        int TID = track.trajectory.TID;
        G2TID glTID = new G2TID(TID);
        if (track.enlargeTuple.f0 != track.passP.get(0)) track.enlargeTuple.f0.bitmap.remove(TID);
        for (Iterator<GDataNode> ite = track.passP.iterator(); ite.hasNext();) {
            GDataNode oldLeaf = ite.next();
            if (delLeaves.contains(oldLeaf)){ //调整后，该节点被废弃
                i--;
                ite.remove();
            }else { //调整后存在该节点
                if (newLeaves.contains(oldLeaf)) { //该节点在调整节点中
                    GDataNode newLeaf = Collections.removeAndGatherElem(trackLeafs.f0, leaf -> leaf.leafID == oldLeaf.leafID);
                    if (newLeaf == null) { //调整后轨迹track不经过这个leafID了
                        out.collect(new G2LElem(oldLeaf.leafID, (byte) 10, glTID));
                        i--;
                        ite.remove();
                    } else { //调整后轨迹track还经过这个leafID了
                        track.passP.set(i, newLeaf);
                    }
                }
            }
            i++;
        }
        for (Iterator<GLeafAndBound> ite = track.topKP.getList().iterator(); ite.hasNext();) {
            GLeafAndBound gb = ite.next();
            if (delLeaves.contains(gb.leaf)) { //调整后，该节点被废弃
                ite.remove();
            }else { //调整后存在该节点
                if (newLeaves.contains(gb.leaf)) { //该节点在调整节点中
                    GDataNode newLeaf = Collections.removeAndGatherElem(trackLeafs.f0, leaf -> leaf.leafID == gb.leaf.leafID);
                    if (newLeaf != null) {
                        out.collect(new G2LElem(newLeaf.leafID, (byte) 9, glTID));
                        track.passP.add(newLeaf);
                    }else {
                        out.collect(new G2LElem(gb.leaf.leafID, (byte) 11, glTID));
                    }
                    ite.remove();
                }
            }
        }
        trackLeafs.f0.forEach(dataNode -> out.collect(new G2LElem(dataNode.leafID, (byte) 12, trackPs)));
        track.passP.addAll(trackLeafs.f0);
        if (track.topKP.isEmpty()){
            pruneIndex.delete(track);
            track.cutOffCandidate(trackMap);
            globalTree.countEnlargeBound(track, track.passP, MBR);
        }else {
            List<GDataNode> list = new ArrayList<>(track.passP);
            list.addAll(Collections.changeCollectionElem(track.topKP.getList(), gb -> gb.leaf));
            globalTree.countTopKAndEnlargeBound(track, track.passP, list, MBR);
        }
        track.enlargeTuple.f0.bitmap.add(TID);
    }



    private boolean check(){
        if (!pruneIndex.check(trackMap))
            return false;
        if (!segmentIndex.check(trackMap))
            return false;
        if (!globalTree.check(trackMap))
            return false;
        for (TrackKeyTID track : trackMap.values()) {
            if (!checkTrack(track))
                return false;
        }
        return true;
    }

    private boolean checkTrack(TrackKeyTID track){
        Integer TID = track.trajectory.TID;

        //trajectory.elms -- segmentIndex检查
        for (Segment segment : track.trajectory.elms) {
            if (!segment.leaf.isInTree())
                return false;
            if (!segment.leaf.elms.contains(segment))
                return false;
        }

        //RelatedInfo 检查
        for (SimilarState key : track.getRelatedInfo().keySet()) {
            SimilarState state = track.getRelatedInfo().get(key);
            int comparedTID = state.getStateAnoTID(TID);
            TrackHauOne comparedTrack = trackMap.get(comparedTID);
            if (key != state)
                return false;
            if (comparedTrack.getSimilarState(TID) != state)
                return false;
            if (!SimilarState.isEquals(state, Hausdorff.getHausdorff(track.trajectory, comparedTrack.trajectory)))
                return false;
            if (!comparedTrack.candidateInfo.contains(TID) && !track.candidateInfo.contains(comparedTID))
                return false;
        }

        //candidateInfo 检查
        if (track.candidateInfo.size() < Constants.topK)
            return false;
        for (Integer comparedTID : track.candidateInfo) {
            if(track.getSimilarState(comparedTID) == null)
                return false;
        }
        for (int i = 0; i < track.candidateInfo.size()-1; i++) {
            SimilarState state1 = track.getSimilarState(track.candidateInfo.get(i));
            SimilarState state2 = track.getSimilarState(track.candidateInfo.get(i+1));
            if (Double.compare(state1.distance, state2.distance) > 0)
                return false;
        }


        Rectangle MBR = track.getPruningRegion(0.0);
        Rectangle pruneArea = MBR.clone().extendLength(track.threshold);

        //rect 检查
        if (!pruneArea.equals(track.rect))
            return false;

        //重新计算
        TrackKeyTID tmpTrack = new TrackKeyTID(null, null, null, null ,TID,null, null);
        Set<Integer> selectedTIDs;  //阈值计算轨迹集
        selectedTIDs = segmentIndex.getInternalNoIPTIDs(pruneArea);
        selectedTIDs.remove(TID);
        if (selectedTIDs.size() < Constants.topK)
            return false;
        List<SimilarState> result = new ArrayList<>(selectedTIDs.size());
        for (Integer comparedTid : selectedTIDs) {
            TrackKeyTID comparedTrack = trackMap.get(comparedTid);
            SimilarState state = track.getSimilarState(comparedTid);
            if (state == null)
                state = Hausdorff.getHausdorff(track.trajectory, comparedTrack.trajectory);
            result.add(state);
        }
        java.util.Collections.sort(result);
        tmpTrack.threshold = result.get(Constants.topK - 1).distance;

        //threshold 检查
        if (track.threshold < tmpTrack.threshold ||
                track.threshold < track.getKCanDistance(Constants.topK).distance ||
                track.getKCanDistance(Constants.topK + Constants.t * 2).distance < track.threshold) {
            return false;
        }else {
            tmpTrack.rect = pruneArea;
            tmpTrack.threshold = track.threshold;
        }

        //enlargeTuple 检查
        globalTree.countPartitions(MBR, tmpTrack);
        if (!track.enlargeTuple.f0.isInTree())
            return false;
        if (!Constants.isEqual(track.enlargeTuple.f1, tmpTrack.enlargeTuple.f1))
            return false;
        if (!track.enlargeTuple.f0.bitmap.contains(TID))
            return false;
        Rectangle rect = track.enlargeTuple.f0.region.extendToEnoughBig();
        if (rect.isIntersection(track.rect)){
            if (!(!rect.isInternal(MBR.clone().extendLength(track.enlargeTuple.f1 + 0.0002)) &&
                    rect.isInternal(MBR.clone().extendLength(track.enlargeTuple.f1 - 0.0002))))
                return false;
        }else {
            if ( !(rect.isIntersection(MBR.clone().extendLength(track.enlargeTuple.f1 + 0.0002)) &&
                    !rect.isIntersection(MBR.clone().extendLength(track.enlargeTuple.f1 - 0.0002))))
                return false;
        }

        //passP 检查
        if (!Collections.collectionsEqual(track.passP, tmpTrack.passP))
            return false;
        for (GDataNode passLeaf : track.passP) {
            if (!passLeaf.isInTree())
                return false;
            if (!passLeaf.bitmap.contains(TID))
                return false;
        }

        //topKP 检查
        if (tmpTrack.topKP.isEmpty()){
            if (!track.topKP.isEmpty())
                return false;
            if (track.threshold > tmpTrack.enlargeTuple.f1)
                return false;
            if (track.threshold < tmpTrack.threshold)
                return false;
            if(track.leaf != null) //pruneIndex检查
                return false;
            if (track.getKCanDistance(Constants.topK).distance > track.threshold)
                return false;
        }else {
            if (track.topKP.isEmpty())
                return false;
            for (GLeafAndBound gb : track.topKP.getList()) {
                if (!gb.leaf.isInTree())
                    return false;
                GLeafAndBound gbb = tmpTrack.topKP.get(gb);
                if (!gbb.leaf.isInTree())
                    return false;
                if (!Constants.isEqual(gb.bound, gbb.bound))
                    return false;
                if (!gb.leaf.bitmap.contains(TID))
                    return false;
            }
            if (!Collections.collectionsEqual(track.topKP.getList(), tmpTrack.topKP.getList()))
                return false;
            if (!track.leaf.isInTree()) //pruneIndex检查
                return false;
            if(!track.leaf.elms.contains(track))
                return false;
            if (!Collections.collectionsEqual(track.candidateInfo, selectedTIDs))
                 return false;
            for (GLeafAndBound gb : track.topKP.getList()) {
                if ( !(gb.leaf.region.isIntersection(MBR.clone().extendLength(gb.bound + 0.0002)) &&
                        !gb.leaf.region.isIntersection(MBR.clone().extendLength(gb.bound - 0.0002))))
                    return false;
            }
        }
        return true;
    }

    private void dealCandidateSmall(TrackKeyTID track) {
        Integer TID = track.trajectory.TID;
        if (count == 14 && TID == 22140)
            System.out.print("");
        if (track.enlargeTuple.f0 != track.passP.get(0)) track.enlargeTuple.f0.bitmap.remove(TID);
        List<SimilarState> list = new ArrayList<>(track.getRelatedInfo().keySet());
        java.util.Collections.sort(list);
        List<Integer> newCanDi = new ArrayList<>();
        for (int i = 0; i < Constants.topK + Constants.t && i < list.size(); i++) {
            newCanDi.add(list.get(i).getStateAnoTID(TID));
        }
        track.candidateInfo.removeAll(newCanDi);
        while (!track.candidateInfo.isEmpty()) track.removeICandidate(track.candidateInfo.size(), trackMap);
        track.candidateInfo = newCanDi;

        Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
        if (track.topKP.isEmpty()){ //没有topK扩展节点
            if (track.candidateInfo.size() < Constants.topK + Constants.t){ //没有足够的候选轨迹
                Rectangle pruneArea = MBR.clone().extendLength(list.get(list.size()-1).distance);
                //筛选出计算阈值的轨迹集合，得出裁剪域
                Set<Integer> selectedTIDs = new HashSet<>();  //阈值计算轨迹集
                while (selectedTIDs.size() < Constants.topK + Constants.t) { //阈值计算轨迹集的元素数要大于Constants.k*Constants.cDTW。
                    selectedTIDs = segmentIndex.getInternalTIDs(pruneArea);
                    pruneArea.extendMultiple(0.3);   //查询轨迹MBR扩展
                }
                selectedTIDs.remove(TID);
                selectedTIDs.removeAll(track.candidateInfo);
                for (Integer selectedTID : selectedTIDs)
                    track.addTrackCandidate(trackMap.get(selectedTID));
                track.sortCandidateInfo();
            }
            double dis = track.getKCanDistance(Constants.topK+Constants.t).distance;
            if (dis > track.enlargeTuple.f1){ //现在需要扩展
                track.rect = extendTopKP(track, MBR, false);
                List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(track.rect);
                if (track.passP.size() == pruneAreaLeafs.size()) { //还没有topK扩展节点
                    track.cutOffCandidate(trackMap);
                    globalTree.countEnlargeBound(track, track.passP, MBR);
                }else { //有topK扩展节点了
                    pruneIndex.insert(track);
                    globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                    outAddTopKTrack(track.topKP.getList(), G2LPoints.toG2LPoints(track.trajectory));
                }
            }else { //不需要扩展
                track.rect = MBR.clone().extendLength(dis);
                track.threshold = dis;
            }
        }else { //有topK扩展节点
            TrackKeyTID tmpTrack = new TrackKeyTID(null, null, null, track.trajectory.elms, TID, new ArrayList<>(), new HashMap<>());
            Rectangle pruneArea = MBR.clone().extendLength(track.getKCanDistance(Constants.topK).distance);
            tmpTrack.rect = DTConstants.newTrackCalculate(tmpTrack, MBR, pruneArea, segmentIndex, trackMap, true, false);
            Map<SimilarState, SimilarState> map = tmpTrack.getRelatedInfo();
            for (SimilarState state : track.getRelatedInfo().values()) {
                if (map.get(state) == null) {
                    TrackKeyTID comparedTrack = trackMap.get(state.getStateAnoTID(TID));
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
            if (pruneAreaLeafs.size() == track.passP.size()){ //无topK扩展节点
                track.cutOffCandidate(trackMap);
                outDeleteTopKPTrackGB(TID, track.topKP.getList());
                pruneIndexRemove(track);
                track.rect = tmpTrack.rect;
                track.data = tmpTrack.rect.getCenter().data;
            }else { //有topK扩展节点
                pruneIndex.alterELem(track, tmpTrack.rect);
                List<GLeafAndBound> oldTopK = track.topKP.getList();
                globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                G2LPoints trackPs = G2LPoints.toG2LPoints(track.trajectory);
                for (GLeafAndBound gb : track.topKP.getList()){
                    if (!oldTopK.remove(gb)) {
                        out.collect(new G2LElem(gb.leaf.leafID, (byte) 3, trackPs));
                        gb.leaf.bitmap.add(TID);
                    }
                }
                outDeleteTopKPTrackGB(TID, oldTopK);
            }
        }
        track.enlargeTuple.f0.bitmap.add(TID);
    }

    /**
     * 轨迹track有采样点滑出窗口，但不是全部。修改相关数据。
     */
    private void processUpdatedTrack(Map<Integer, List<TrackPoint>> inPointsMap,
                                     Set<TrackKeyTID> pruneChangeTracks,
                                     Set<TrackKeyTID> canSmallTracks) {
        RoaringBitmap calculatedTIDs = new RoaringBitmap();
        for (Integer tid : outTIDs) {
            TrackKeyTID track = trackMap.get(tid);
            if (count == 12 && tid == 701)
                System.out.print("");
            for (SimilarState state : track.getRelatedInfo().values()) {
                int comparedTid = state.getStateAnoTID(tid);
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
            if (count == 14 && tid == 2357)
                System.out.print("");
            List<TrackPoint> inPoints = inPointsMap.get(tid);
            Rectangle pointsMBR = Rectangle.pointsMBR(inPoints.toArray(new Point[0]));
            if (track.trajectory.elms.size() == inPoints.size() - 1){ //新的轨迹
                Rectangle pruneArea = pointsMBR.clone().extendLength(Constants.extend);
                track.rect = DTConstants.newTrackCalculate(track, pointsMBR, pruneArea, segmentIndex, trackMap, false, true);
                track.data = track.rect.getCenter().data;
                globalTree.countPartitions(pointsMBR, track);
                track.enlargeTuple.f0.bitmap.add(tid);
                G2LPoints trackPs = G2LPoints.toG2LPoints(track.trajectory);
                if (!track.topKP.isEmpty()) {
                    outAddTopKTrack(track.topKP.getList(), trackPs);
                    pruneIndex.insert(track);
                }else {
                    track.cutOffCandidate(trackMap);
                }
                track.passP.forEach(dataNode -> {
                    out.collect(new G2LElem(dataNode.leafID, (byte) 2, trackPs));
                    dataNode.bitmap.add(tid);
                });
            }else {
                pointsMBR.getUnionRectangle(track.trajectory.elms.getLast());
                for (Iterator<Map.Entry<SimilarState, SimilarState>> ite = track.getRelatedInfo().entrySet().iterator(); ite.hasNext();){
                    SimilarState state = ite.next().getValue();
                    int comparedTid = state.getStateAnoTID(tid);
                    if (tid == 28780 && comparedTid == 28054)
                        System.out.print("");
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
                            if (addPruneChangeAndCanSmall(tid, comparedTrack, pointsMBR, pruneChangeTracks, canSmallTracks)){
                                if (!track.candidateInfo.contains(comparedTid)) {
                                    comparedTrack.getRelatedInfo().remove(state);
                                    ite.remove();
                                }
                            }
                        }
                    }
                }
                track.sortCandidateInfo();
                recalculateInPointTrack(track, inPoints, pointsMBR);
            }
            calculatedTIDs.add(tid);
        }

        for (Integer tid : inAndOutTIDs) {
            TrackKeyTID track = trackMap.get(tid);
            if (count == 17 && tid == 18058)
                System.out.print("");
            List<TrackPoint> inPoints = inPointsMap.get(tid);
            Rectangle pointsMBR = Rectangle.pointsMBR(inPoints.toArray(new Point[0]));
            pointsMBR.getUnionRectangle(track.trajectory.elms.getLast());
            for (Iterator<Map.Entry<SimilarState, SimilarState>> ite = track.getRelatedInfo().entrySet().iterator(); ite.hasNext();){
                SimilarState state = ite.next().getValue();
                int comparedTid = state.getStateAnoTID(tid);
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
                        if (addPruneChangeAndCanSmall(tid, comparedTrack, pointsMBR, pruneChangeTracks, canSmallTracks)){
                            if (!track.candidateInfo.contains(comparedTid)) {
                                comparedTrack.getRelatedInfo().remove(state);
                                ite.remove();
                            }
                        }
                    }
                }
            }
            track.sortCandidateInfo();
            recalculateInAndOutPointTrack(track, inPoints);
            calculatedTIDs.add(tid);
        }
    }

    private boolean addPruneChangeAndCanSmall(Integer tid,
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
                return true;
            }
        }
        return false;
    }

    private void recalculateInPointTrack(TrackKeyTID track, List<TrackPoint> points, Rectangle pointsMBR) {
        Integer TID = track.trajectory.TID;
        if (track.enlargeTuple.f0 != track.passP.get(0)) track.enlargeTuple.f0.bitmap.remove(TID);
        Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
        MBR = MBR.getUnionRectangle(pointsMBR);
        boolean hasNotTopK = track.topKP.isEmpty();

        //往track经过的节点发送数据
        G2LPoints addPs = new G2LPoints(points);
        sendG2LForAddPs(track, addPs, MBR);
        recalculateTrack(track, MBR, hasNotTopK, addPs, new ArrayList<>(1));
        track.enlargeTuple.f0.bitmap.add(TID);
    }

    private void recalculateOutPointTrack(TrackKeyTID track) {
        try {
            Integer TID = track.trajectory.TID;
            if (track.enlargeTuple.f0 != track.passP.get(0)) track.enlargeTuple.f0.bitmap.remove(TID);
            Rectangle MBR = track.getPruningRegion(0.0);
            List<GDataNode> deleteLeafs = (List<GDataNode>) Collections.removeAndGatherElms(track.passP, leaf -> !leaf.region.isIntersection(MBR));
            recalculateTrack(track, MBR, track.topKP.isEmpty(), null, deleteLeafs);
            outDeletePassTrack(TID, deleteLeafs);
            track.enlargeTuple.f0.bitmap.add(TID);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void recalculateInAndOutPointTrack(TrackKeyTID track,
                                               List<TrackPoint> points){
        try {
            Integer TID = track.trajectory.TID;
            if (track.enlargeTuple.f0 != track.passP.get(0)) track.enlargeTuple.f0.bitmap.remove(TID);
            Rectangle MBR = track.getPruningRegion(0.0);
            boolean hasNotTopK = track.topKP.isEmpty();
            //往track经过的节点发送数据
            List<GDataNode> deleteLeafs = (List<GDataNode>) Collections.removeAndGatherElms(track.passP, leaf -> !leaf.region.isIntersection(MBR));
            G2LPoints addPs = new G2LPoints(points);
            sendG2LForAddPs(track, addPs, MBR);
            recalculateTrack(track, MBR, hasNotTopK, addPs, deleteLeafs);
            outDeletePassTrack(TID, deleteLeafs);
            track.enlargeTuple.f0.bitmap.add(TID);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    /**
     * 轨迹track发生了采样点的添加或移除，重新track的topK节点等相关操作。
     */
    private void recalculateTrack(TrackKeyTID track,
                                  Rectangle MBR,
                                  boolean hasNotTopK,
                                  G2LPoints addPs,
                                  List<GDataNode> deleteLeafs) {
        Integer TID = track.trajectory.TID;
        if (track.candidateInfo.size() < Constants.topK + Constants.t)
            DTConstants.supplyCandidate(track, MBR, trackMap, null, segmentIndex, true);
        G2LPoints trackPs = G2LPoints.toG2LPoints(track.trajectory);
        double newThreshold = track.getKCanDistance(Constants.topK + Constants.t).distance;
        if (hasNotTopK) { //没有topK扩展节点
            Rectangle pruneArea = MBR.clone().extendLength(newThreshold);
            List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
            if (track.passP.size() == pruneAreaLeafs.size()){ //还没有topK扩展节点
                track.rect = pruneArea;
                track.data = pruneArea.getCenter().data;
                track.threshold = newThreshold;
                globalTree.countEnlargeBound(track, track.passP, MBR);
            }else { //有topK扩展节点了
                double oldThreshold = track.threshold;
                pruneArea = extendTopKP(track, MBR, true);
                if (oldThreshold < track.threshold){
                    pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
                }else {
                    Rectangle finalArea = pruneArea;
                    pruneAreaLeafs.removeIf(leaf -> !finalArea.isIntersection(leaf.region));
                }
                track.rect = pruneArea;
                track.data = pruneArea.getCenter().data;
                if (track.passP.size() == pruneAreaLeafs.size()) { //还没有topK扩展节点
                    track.cutOffCandidate(trackMap);
                    globalTree.countEnlargeBound(track, track.passP, MBR);
                }else { //有topK扩展节点了
                    pruneIndex.insert(track);
                    globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                    for (GLeafAndBound gb : track.topKP.getList()) {
                        if (deleteLeafs.remove(gb.leaf)) {
                            if (addPs != null)
                                out.collect(new G2LElem(gb.leaf.leafID, (byte) 0, addPs));
                            out.collect(new G2LElem(gb.leaf.leafID, (byte) 6, new G2TID(TID)));
                        } else {
                            out.collect(new G2LElem(gb.leaf.leafID, (byte) 3, trackPs));
                            gb.leaf.bitmap.add(TID);
                        }
                    }
                }
            }
        }else { //有topK扩展节点
            Rectangle pruneArea = DTConstants.recalculateTrackTopK(track, MBR, segmentIndex, trackMap, true);
            List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(pruneArea);
            if (pruneAreaLeafs.size() == track.passP.size()){ //没有topK扩展节点
                track.cutOffCandidate(trackMap);
                outDeleteTopKPTrackGB(TID, track.topKP.getList());
                pruneIndex.delete(track);
                track.topKP.clear();
                track.rect = pruneArea;
                track.data = pruneArea.getCenter().data;
                globalTree.countEnlargeBound(track, track.passP, MBR);
            }else { //有topK扩展节点
                pruneIndex.alterELem(track, pruneArea);
                List<GLeafAndBound> oldTopK = track.topKP.getList();
                globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                for (GLeafAndBound gb : track.topKP.getList()){
                    if (oldTopK.remove(gb)) {
                        if (addPs != null)
                            out.collect(new G2LElem(gb.leaf.leafID, (byte) 1, addPs));
                    }else {
                        if (deleteLeafs.remove(gb.leaf)){
                            if (addPs != null)
                                out.collect(new G2LElem(gb.leaf.leafID, (byte) 0, addPs));
                            out.collect(new G2LElem(gb.leaf.leafID, (byte) 6, new G2TID(TID)));
                        }else {
                            out.collect(new G2LElem(gb.leaf.leafID, (byte) 3, trackPs));
                            gb.leaf.bitmap.add(TID);
                        }
                    }
                }
                outDeleteTopKPTrackGB(TID, oldTopK);
            }
        }
    }

    /**
     * 轨迹track新添加了一些采样点addPs， 会产生一些新的passP节点，
     * 向这些新增的passP节点和原有的passP节点中发送track的采样点信息。
     */
    private void sendG2LForAddPs(TrackKeyTID track,
                                 G2LPoints addPs,
                                 Rectangle MBR) {
        List<GDataNode> newMBRLeafs = globalTree.getIntersectLeafNodes(MBR);
        Integer TID = track.trajectory.TID;
        G2LPoints trackPs = G2LPoints.toG2LPoints(track.trajectory);
        track.passP.forEach(dataNode -> out.collect(new G2LElem(dataNode.leafID, (byte) 0, addPs)));
        newMBRLeafs.removeAll(track.passP);
        if (!newMBRLeafs.isEmpty()) {
            if (track.topKP.isEmpty()) {
                for (GDataNode leaf : newMBRLeafs) {
                    out.collect(new G2LElem(leaf.leafID, (byte) 2, trackPs));
                    leaf.bitmap.add(TID);
                }
            } else {
                List<GLeafAndBound> topKLeafs = track.topKP.getList();
                for (GDataNode leaf : newMBRLeafs) {
                    GLeafAndBound gb = new GLeafAndBound(leaf, 0.0);
                    if (topKLeafs.contains(gb)) {
                        out.collect(new G2LElem(leaf.leafID, (byte) 1, addPs));
                        out.collect(new G2LElem(leaf.leafID, (byte) 7, new G2TID(TID)));
                        topKLeafs.remove(gb);
                    } else {
                        out.collect(new G2LElem(leaf.leafID, (byte) 2, trackPs));
                        leaf.bitmap.add(TID);
                    }
                }
            }
            track.passP.addAll(newMBRLeafs);
        }
    }

    private void pruneIndexRemove(TrackKeyTID track){
        pruneIndex.delete(track);
        GLeafAndBound gB = track.topKP.getFirst();
        track.enlargeTuple.f0 = gB.leaf;
        track.enlargeTuple.f1 = gB.bound;
        track.topKP.clear();
    }

    private Rectangle extendTopKP(TrackKeyTID track, Rectangle MBR, boolean hasAlter) {
        Integer TID = track.trajectory.TID;
        SimilarState thresholdState = track.getKCanDistance(Constants.topK + Constants.t);
        double threshold = thresholdState.distance;
        Rectangle pruneArea = MBR.clone().extendLength(threshold);
        //用裁剪域筛选出候选轨迹集，计算距离并排序
        Set<Integer> needCompareTIDS = segmentIndex.getInternalNoIPTIDs(pruneArea);
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
                int comparedTID = state.getStateAnoTID(TID);
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
                int comparedTID = state.getStateAnoTID(TID);
                track.candidateInfo.add(comparedTID);
                state = trackMap.get(comparedTID).putRelatedInfo(state);
                track.putRelatedInfo(state);

            }
            Map<SimilarState, SimilarState> map = track.getRelatedInfo();
            Judge<TrackKeyTID> judge;
            if (hasAlter){
                judge = t -> t.outSideRectangle(pruneArea);
            }else {
                judge = t -> !pruneArea.isInternal(t.rect.clone().extendLength(-t.threshold));
            }
            for (int i = Constants.topK + Constants.t; i < needCompareState.size(); i++) {
                SimilarState state = needCompareState.get(i);
                Integer comparedTID = state.getStateAnoTID(TID);
                TrackKeyTID comparedTrack = trackMap.get(comparedTID);
                if (judge.action(comparedTrack)) {
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
                    track.rect = extendTopKP(track, MBR, false);
                    List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(track.rect);
                    if (track.passP.get(0) != track.enlargeTuple.f0) track.enlargeTuple.f0.bitmap.remove(TID);
                    if (track.passP.size() == pruneAreaLeafs.size()) { //还没有topK扩展节点
                        track.cutOffCandidate(trackMap);
                        globalTree.countEnlargeBound(track, track.passP, MBR);
                    }else { //有topK扩展节点了
                        pruneIndex.insert(track);
                        globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                        outAddTopKTrack(track.topKP.getList(), G2LPoints.toG2LPoints(track.trajectory));
                    }
                    track.enlargeTuple.f0.bitmap.add(TID);
                }else {  //无topK扩展节点，现在不需要扩展
                    track.rect.extendLength(-track.threshold + dis);
                    track.threshold = dis;
                }
            }else{  //有topK扩展节点
                if (track.passP.get(0) != track.enlargeTuple.f0) track.enlargeTuple.f0.bitmap.remove(TID);
                Rectangle pruneArea = DTConstants.enlargePrune(track, dis, segmentIndex, trackMap);
                if (track.enlargeTuple.f1 < track.threshold) { //topK扩展节点变多
                    pruneIndex.alterELem(track, pruneArea);
                    List<GDataNode> leafs = globalTree.enlargePartitions(track, MBR);
                    //通知扩展
                    G2LPoints trackPs = G2LPoints.toG2LPoints(track.trajectory);
                    leafs.forEach(leaf -> {
                        out.collect(new G2LElem(leaf.leafID, (byte) 3, trackPs));
                        leaf.bitmap.add(TID);
                    });
                }else{
                    if (track.topKP.getLast().bound >= track.threshold) { //topK扩展节点变少
                        if (track.topKP.getFirst().bound < track.threshold){ //topK扩展节点变少
                            pruneIndex.alterELem(track, pruneArea);
                            List<GLeafAndBound> minusTopKP = track.minusTopKP();
                            outDeleteTopKPTrackGB(TID, minusTopKP);
                        }else {  //topK扩展节点变没
                            outDeleteTopKPTrackGB(TID, track.topKP.getList());
                            pruneIndexRemove(track);
                            track.rect = pruneArea;
                            track.cutOffCandidate(trackMap);
                        }
                    }else {
                        pruneIndex.alterELem(track, pruneArea);
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
                Rectangle pruneArea = DTConstants.tightenThresholdCommon(track, trackMap);
                if (track.threshold < track.topKP.getLast().bound){ //topK扩展节点变少
                    if (track.threshold < track.topKP.getFirst().bound){ //topK扩展节点变没
                        outDeleteTopKPTrackGB(TID, track.topKP.getList());
                        pruneIndexRemove(track);
                        track.rect = pruneArea;
                        track.cutOffCandidate(trackMap);
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
                comparedTrack.addTrackCandidate(track);
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
            out.collect(new G2LElem(gB.leaf.leafID, (byte) 5, new G2TID(TID)));
        });
    }

    private void forInitCode(int[][] density, long logicWinStart) {
        if (density != null) {
            densityQue.add(density);
            Arrays.addArrsToArrs(globalTree.density, density, true);
            globalTree.updateTree();
        }
        if (tIDsQue.size() > Constants.logicWindow){ //窗口完整后才能进行初始化计算
            if (tIDsQue.element().f0 < logicWinStart) {
                for (Integer tid : tIDsQue.remove().f1) {
                    for (Segment segment : trackMap.get(tid).trajectory.removeElem(logicWinStart)) {
                        segmentIndex.delete(segment);
                    }
                }
            }
            for (TrackKeyTID track : trackMap.values()) {
                track.rect = track.getPruningRegion(0.0);
                track.data = track.rect.getCenter().data;
                for (GDataNode leaf : globalTree.getIntersectLeafNodes(track.rect)) {
                    leaf.bitmap.add(track.trajectory.TID);
                }
            }

            // globalTree中的每个叶节点中的轨迹数量超过Constants.topK才能进行初始化计算
            boolean canInit = true;
            for (GDataNode leaf : globalTree.getAllLeafs()) {
                if (leaf.bitmap.toArray().length <= Constants.topK * Constants.KNum){
                    canInit = false;
                    break;
                }
            }
            if (canInit){
                hasInit = true;
                initCalculate();
                globalTree.getAllLeafs().forEach(leaf -> out.collect(new G2LElem(leaf.leafID, (byte) 14, leaf.region)));
                for (TrackKeyTID track : trackMap.values()) {
                    G2LPoints trackPs = G2LPoints.toG2LPoints(track.trajectory);
                    for (GDataNode leaf : track.passP) {
                        out.collect(new G2LElem(leaf.leafID, (byte) 15, trackPs));
                    }
                    if (!track.topKP.isEmpty()) {
                        for (GLeafAndBound gb : track.topKP.getList()) {
                            out.collect(new G2LElem(gb.leaf.leafID, (byte) 16, trackPs));
                        }
                    }
                }
            }else {
                for (GDataNode leaf : globalTree.getAllLeafs()) leaf.bitmap.clear();
            }
        }
    }

    private void initCalculate(){
        for (TrackKeyTID track : trackMap.values()) {
            int TID = track.trajectory.TID;
            List<TrackKeyTID> list = new ArrayList<>(trackMap.values());
            list.remove(track);
            for (TrackKeyTID comparedTrack : list)
                track.addTrackCandidate(comparedTrack);
            track.sortCandidateInfo();
            track.threshold = track.getKCanDistance(Constants.t + Constants.topK).distance;
            Rectangle MBR = track.rect.clone();
            track.rect = MBR.clone().extendLength(track.threshold);
            globalTree.countPartitions(MBR, track);
            track.enlargeTuple.f0.bitmap.add(TID);
            G2LPoints trackPs = G2LPoints.toG2LPoints(track.trajectory);
            if (track.topKP.isEmpty()) {
               track.cutOffCandidate(trackMap);
            }else {
                track.topKP.getList().forEach(gB -> {
                    out.collect(new G2LElem(gB.leaf.leafID, (byte) 13, trackPs));
                    gB.leaf.bitmap.add(TID);
                });
                pruneIndex.insert(track);
            }
            for (int i = Constants.t + Constants.topK; i < track.candidateInfo.size(); i++) {
                TrackKeyTID comparedTrack = trackMap.get(track.candidateInfo.get(i));
                if (!track.rect.isInternal(comparedTrack.rect.clone().extendLength(-comparedTrack.threshold))){
                    track.removeTIDCandidate(comparedTrack);
                    i--;
                }
            }
            track.passP.forEach(dataNode -> {
                out.collect(new G2LElem(dataNode.leafID, (byte) 12, trackPs));
                dataNode.bitmap.add(TID);
            });
        }
    }

    /**
     * 通知list中的对应的divide节点,添加一条新的topK轨迹trackMessage
     */
    private void outAddTopKTrack(List<GLeafAndBound> list, G2LPoints track) {
        int TID = track.points.get(0).TID;
        list.forEach(gB -> {
            out.collect(new G2LElem(gB.leaf.leafID, (byte) 3, track));
            gB.leaf.bitmap.add(TID);
        });
    }

    /**
     * 通知removeList中的对应的divide节点删除topK轨迹comparedTid
     */
    private void outDeletePassTrack(int TID, List<GDataNode> leafs) {
        G2TID p = new G2TID(TID);
        leafs.forEach(leaf -> {
            leaf.bitmap.remove(TID);
            out.collect(new G2LElem(leaf.leafID, (byte) 4, p));
        });
    }

    /**
     * 对窗口输入数据elements进行预处理，包括识别并统计出密度网格、将轨迹
     * 点按照轨迹ID分类到Map中、统计所有的轨迹ID到RoaringBitmap中、将轨
     * 迹点添加到轨迹中。
     */
    private int[][] preElements(Iterable<D2GElem> elements,
                                List<TrackKeyTID> newTracks,
                                Map<Integer , List<TrackPoint>> inPointsMap) {
        List<Density> densities = new ArrayList<>(Constants.globalPartition);
        for (D2GElem element : elements) {
            if (element instanceof TrackPoint){
                TrackPoint point = (TrackPoint) element;
                inPointsMap.computeIfAbsent(point.TID, tid -> new ArrayList<>(4)).add(point);
            }else {
                densities.add((Density) element);
            }
        }
        for (int i = 1; i < densities.size(); i++)
            Arrays.addArrsToArrs(densities.get(0).grids, densities.get(i).grids, true);

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
                track = new TrackKeyTID(null,
                        null,
                        null,
                        new ArrayQueue<>(segments),
                        entry.getKey(),
                        new ArrayList<>(),
                        new HashMap<>());
                trackMap.put(entry.getKey(), track);
                newTracks.add(track);
            }else { //已有的轨迹
                if (entry.getValue().size() == 1){
                    segments = new ArrayList<>(1);
                    segments.add(new Segment(track.trajectory.elms.getLast().p2, entry.getValue().get(0)));
                }else {
                    segments = Segment.pointsToSegments(entry.getValue());
                    segments.add(0, new Segment(track.trajectory.elms.getLast().p2, entry.getValue().get(0)));
                }
                track.trajectory.addSegments(segments);
            }
            for (Segment segment : segments) segmentIndex.insert(segment);
        }
        if (densities.isEmpty())
            return null;
        else
            return densities.get(0).grids;
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
        jedis = new Jedis("localhost");
    }
}
