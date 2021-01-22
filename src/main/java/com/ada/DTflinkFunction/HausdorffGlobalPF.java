package com.ada.DTflinkFunction;

import com.ada.Hausdorff.SimilarState;
import com.ada.QBSTree.RCtree;
import com.ada.common.ArrayQueue;
import com.ada.common.SortList;
import com.ada.common.collections.Collections;
import com.ada.common.Arrays;
import com.ada.common.Constants;
import com.ada.Hausdorff.Hausdorff;
import com.ada.geometry.*;
import com.ada.geometry.track.TrackHauOne;
import com.ada.geometry.track.TrackKeyTID;
import com.ada.globalTree.GDataNode;
import com.ada.globalTree.GDirNode;
import com.ada.globalTree.GNode;
import com.ada.globalTree.GTree;
import com.ada.model.densityToGlobal.Density;
import com.ada.model.densityToGlobal.Density2GlobalElem;
import com.ada.model.globalToLocal.Global2LocalElem;
import com.ada.model.globalToLocal.Global2LocalPoints;
import com.ada.model.globalToLocal.Global2LocalTID;
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
        if (count >= 15)
            System.out.print("");
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
            RoaringBitmap outTIDs = tIDsQue.remove();
            RoaringBitmap inAndOutTIDs = inputIDs.clone();
            inAndOutTIDs.and(outTIDs);
            RoaringBitmap inTIDs = inputIDs.clone();
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
            for (Integer tid : emptyTracks) {
                TrackKeyTID track = trackMap.get(tid);
                track.enlargeTuple.f0.bitmap.remove(tid);
                for (GDataNode leaf : track.passP) leaf.bitmap.remove(tid);
                if (!track.topKP.isEmpty()){
                    for (GLeafAndBound gb : track.topKP.getList()) gb.leaf.bitmap.remove(tid);
                }
                DTConstants.dealAllSlideOutTracks(track, inTIDs, outTIDs, inAndOutTIDs, pruneChangeTracks, emptyTracks, canSmallTracks, trackMap,null, pruneIndex);
            }
            for (Integer tid : emptyTracks) trackMap.remove(tid);
            //处理整条轨迹未完全滑出窗口的轨迹
            processUpdatedTrack(inTIDs, outTIDs, inAndOutTIDs, inPointsMap, pruneChangeTracks, canSmallTracks);
            pruneChangeTracks.removeAll(canSmallTracks);
            for (TrackKeyTID track : pruneChangeTracks) changeThreshold(track);
            for (TrackKeyTID track : canSmallTracks) dealCandidateSmall(track);
            for (Integer tid : outTIDs) mayBeAnotherTopK(trackMap.get(tid));
            for (Integer tid : inAndOutTIDs) mayBeAnotherTopK(trackMap.get(tid));
            for (TrackKeyTID track : newTracks) mayBeAnotherTopK(track);

            // 调整Global Index, 然后将调整结果同步到相关的Local Index中。
            if (density != null){
                densityQue.add(density);
                Arrays.addArrsToArrs(globalTree.density, density, true);
                if (densityQue.size() > Constants.logicWindow / Constants.densityFre) {
                    int[][] removed = densityQue.remove();
                    Arrays.addArrsToArrs(globalTree.density, removed, false);
                }

                //调整Global Index
                Map<GNode, GNode> nodeMap = globalTree.updateTree();

                //Global Index发生了调整，通知Local Index迁移数据，重建索引。
                if (!nodeMap.isEmpty()){
                    if (subTask == 0) {
                        //通知Local Index其索引区域发生的变化
                        adjustLocalTasksRegion(nodeMap);
                    }
                    redispatchGNode(nodeMap);
                    check();
                }

            }
            if (count % 20 == 0) {
                System.out.println(count);
                check();
            }
        }else { //轨迹足够多时，才开始计算，计算之间进行初始化
            if (density != null)
                globalTree.updateTree();
            if (tIDsQue.size() > Constants.logicWindow){ //窗口完整后才能进行初始化计算
                for (Integer tid : tIDsQue.remove()) {
                    for (Segment segment : trackMap.get(tid).trajectory.removeElem(logicWinStart)) {
                        segmentIndex.delete(segment);
                    }
                }
                for (TrackKeyTID track : trackMap.values()) {
                    track.rect = Constants.getPruningRegion(track.trajectory, 0.0);
                    track.data = track.rect.getCenter().data;
                    for (GDataNode leaf : globalTree.getIntersectLeafNodes(track.rect)) {
                        leaf.bitmap.add(track.trajectory.TID);
                    }
                }

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
                }else {
                    for (GDataNode leaf : globalTree.getAllLeafs()) leaf.bitmap.clear();
                }
            }
        }
        count++;
    }

    private void adjustLocalTasksRegion(Map<GNode, GNode> nodeMap){
        Map<Integer, LocalRegionAdjustInfo> migrateOutMap = new HashMap<>();
        Map<Integer, LocalRegionAdjustInfo> migrateFromMap = new HashMap<>();
        for (Map.Entry<GNode, GNode> entry : nodeMap.entrySet()) {
            List<GDataNode> leafs = new ArrayList<>();
            entry.getKey().getLeafs(leafs);
            for (GDataNode oldLeaf : leafs) {
                List<GDataNode> migrateOutLeafs = new ArrayList<>();
                entry.getValue().getIntersectLeafNodes(oldLeaf.region, migrateOutLeafs);
                migrateOutLeafs.removeIf(leaf -> leaf.leafID == oldLeaf.leafID);
                List<Tuple2<Integer, Rectangle>> migrateOutLeafIDs;
                migrateOutLeafIDs = (List<Tuple2<Integer, Rectangle>>) Collections.changeCollectionElem(migrateOutLeafs, node -> new Tuple2<>(node.leafID,node.region));
                migrateOutMap.put(oldLeaf.leafID, new LocalRegionAdjustInfo(migrateOutLeafIDs, null, null));
            }
            leafs.clear();
            entry.getValue().getLeafs(leafs);
            for (GDataNode newLeaf : leafs) {
                List<GDataNode> migrateFromLeafs = new ArrayList<>();
                entry.getKey().getIntersectLeafNodes(newLeaf.region, migrateFromLeafs);
                List<Integer> migrateFromLeafIDs = (List<Integer>) Collections.changeCollectionElem(migrateFromLeafs, node -> node.leafID);
                migrateFromLeafIDs.remove(new Integer(newLeaf.leafID));
                migrateFromMap.put(newLeaf.leafID, new LocalRegionAdjustInfo(null, migrateFromLeafIDs, newLeaf.region));
            }
        }
        Iterator<Map.Entry<Integer, LocalRegionAdjustInfo>> ite = migrateOutMap.entrySet().iterator();
        while (ite.hasNext()){
            Map.Entry<Integer, LocalRegionAdjustInfo> entry = ite.next();
            LocalRegionAdjustInfo elem = migrateFromMap.get(entry.getKey());
            if (elem != null){
                elem.setMigrateOutST(entry.getValue().getMigrateOutST());
                ite.remove();
            }
        }
        migrateFromMap.putAll(migrateOutMap);
        for (Map.Entry<Integer, LocalRegionAdjustInfo> entry : migrateFromMap.entrySet())
            out.collect(new GlobalToLocalElem(entry.getKey(), 3, entry.getValue()));
    }

    private void redispatchGNode(Map<GNode, GNode> map) {
        //记录轨迹的enlarge node bound在在修改节点集中的轨迹
        RoaringBitmap enlargeTIDs = new RoaringBitmap();

        //记录轨迹在调整后的节点newNode中的经过节点和topK节点
        //key：轨迹ID
        //value：该轨迹需要出现的节点{tuple0:为经过节点，tuple1为topK节点}
        Map<Integer, Tuple2<List<GDataNode>,List<GDataNode>>> trackLeafsMap = new HashMap<>();
        map.forEach((oldNode, newNode) -> {
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
        });
        map.forEach((oldNode, newNode) -> {
            Map<Integer, Tuple2<List<GDataNode>,List<GDataNode>>> trackLeafsInnerMap = new HashMap<>();
            List<GDataNode> oldLeafs = new ArrayList<>();
            oldNode.getLeafs(oldLeafs);
            List<GDataNode> newLeafs = new ArrayList<>();
            newNode.getLeafs(newLeafs);
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


        Collection<GDataNode> allLeafs = globalTree.getAllLeafs();
        trackLeafsMap.forEach((TID, trackLeafs) -> {
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



    private boolean check(){
        if (!pruneIndex.check(trackMap))
            return false;
        if (!segmentIndex.check(trackMap))
            return false;
        if (!globalTree.check(trackMap))
            return false;
        for (TrackKeyTID track : trackMap.values()) {
            if (!trackCheck(track))
                return false;
        }
        return true;
    }

    private boolean trackCheck(TrackKeyTID track){
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
            int comparedTID = Constants.getStateAnoTID(state, TID);
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


        Rectangle MBR = Constants.getPruningRegion(track.trajectory, 0.0);
        Rectangle pruneArea = MBR.clone().extendLength(track.threshold);

        //rect 检查
        if (!pruneArea.equals(track.rect))
            return false;

        //重新计算
        TrackKeyTID tmpTrack = new TrackKeyTID(null, null, null, null ,TID,null, null);
        Set<Integer> selectedTIDs;  //阈值计算轨迹集
        selectedTIDs = segmentIndex.getRegionInternalTIDs(pruneArea);
        selectedTIDs.remove(TID);
        if (selectedTIDs.size() < Constants.topK)
            return false;
        List<SimilarState> result = new  ArrayList<>();
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
            if (!Collections.collectionsEqual(track.topKP.getList(), tmpTrack.topKP.getList()))
                return false;
            for (GLeafAndBound gb : track.topKP.getList()) {
                GLeafAndBound gbb = tmpTrack.topKP.get(gb);
                if (!Constants.isEqual(gb.bound, gbb.bound))
                    return false;
                if (!gb.leaf.bitmap.contains(TID))
                    return false;
            }
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

        Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
        if (track.topKP.isEmpty()){ //没有topK扩展节点
            if (newCanDi.size() < Constants.topK + Constants.t){ //没有足够的候选轨迹
                Rectangle pruneArea = MBR.clone().extendLength(list.get(list.size()-1).distance);
                //筛选出计算阈值的轨迹集合，得出裁剪域
                Set<Integer> selectedTIDs = new HashSet<>();  //阈值计算轨迹集
                while (selectedTIDs.size() < Constants.topK + Constants.t) { //阈值计算轨迹集的元素数要大于Constants.k*Constants.cDTW。
                    selectedTIDs = segmentIndex.getIntersectTIDs(pruneArea);
                    pruneArea.extendMultiple(0.3);   //查询轨迹MBR扩展
                }
                selectedTIDs.removeAll(newCanDi);
                for (Integer selectedTID : selectedTIDs)
                    track.addTrackCandidate(trackMap.get(selectedTID));
            }
            double dis = list.get(Constants.topK+Constants.t-1).distance;
            if (dis > track.enlargeTuple.f1){ //现在需要扩展
                track.rect = extendTopKP(track, MBR);
                List<GDataNode> pruneAreaLeafs = globalTree.getIntersectLeafNodes(track.rect);
                if (track.passP.size() == pruneAreaLeafs.size()) { //还没有topK扩展节点
                    track.cutOffCandidate(trackMap);
                    globalTree.countEnlargeBound(track, track.passP, MBR);
                }else { //有topK扩展节点了
                    pruneIndex.insert(track);
                    globalTree.countTopKAndEnlargeBound(track, track.passP, pruneAreaLeafs, MBR);
                    outAddTopKTrackGB(track.topKP.getList(), Global2LocalPoints.ToG2LPoints(track.trajectory));
                }
            }else { //不需要扩展
                track.rect = MBR.clone().extendLength(dis);
                track.threshold = dis;
            }
        }else { //有topK扩展节点
            TrackKeyTID tmpTrack = new TrackKeyTID(null, null, null, track.trajectory.elms, TID, new ArrayList<>(), new HashMap<>());
            Rectangle pruneArea = MBR.clone().extendLength(track.getKCanDistance(Constants.topK).distance);
            tmpTrack.rect = DTConstants.newTrackCalculate(tmpTrack, MBR, pruneArea, segmentIndex, trackMap);
            Map<SimilarState, SimilarState> map = tmpTrack.getRelatedInfo();
            for (SimilarState state : track.getRelatedInfo().values()) {
                if (map.get(state) == null) {
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
                Global2LocalPoints trackPs = Global2LocalPoints.ToG2LPoints(track.trajectory);
                for (GLeafAndBound gb : track.topKP.getList()){
                    if (!oldTopK.remove(gb)) {
                        out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 3, trackPs));
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
    private void processUpdatedTrack(RoaringBitmap inTIDs,
                                     RoaringBitmap outTIDs,
                                     RoaringBitmap inAndOutTIDs,
                                     Map<Integer, List<TrackPoint>> inPointsMap,
                                     Set<TrackKeyTID> pruneChangeTracks,
                                     Set<TrackKeyTID> canSmallTracks) {
        RoaringBitmap calculatedTIDs = new RoaringBitmap();
        for (Integer tid : outTIDs) {
            TrackKeyTID track = trackMap.get(tid);
            if (count == 15 && tid == 34864)
                System.out.print("");
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
            if (count == 14 && tid == 2357)
                System.out.print("");
            List<TrackPoint> inPoints = inPointsMap.get(tid);
            Rectangle pointsMBR = Rectangle.pointsMBR(inPoints.toArray(new Point[0]));
            if (track.trajectory.elms.size() == inPoints.size() - 1){ //新的轨迹
                Rectangle pruneArea = pointsMBR.clone().extendLength(Constants.extend);
                track.rect = DTConstants.newTrackCalculate(track, pointsMBR, pruneArea, segmentIndex, trackMap);
                track.data = track.rect.getCenter().data;
                globalTree.countPartitions(pointsMBR, track);
                track.enlargeTuple.f0.bitmap.add(tid);
                Global2LocalPoints trackPs = Global2LocalPoints.ToG2LPoints(track.trajectory);
                if (!track.topKP.isEmpty()) {
                    outAddTopKTrackGB(track.topKP.getList(), trackPs);
                    pruneIndex.insert(track);
                }else {
                    track.cutOffCandidate(trackMap);
                }
                outAddPassTrack(track.passP, trackPs);
            }else {
                pointsMBR.getUnionRectangle(track.trajectory.elms.getLast());
                for (Iterator<Map.Entry<SimilarState, SimilarState>> ite = track.getRelatedInfo().entrySet().iterator(); ite.hasNext();){
                    SimilarState state = ite.next().getValue();
                    int comparedTid = Constants.getStateAnoTID(state, tid);
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
            if (tid == 6146 || tid == 11482)
                System.out.print("");
            List<TrackPoint> inPoints = inPointsMap.get(tid);
            Rectangle pointsMBR = Rectangle.pointsMBR(inPoints.toArray(new Point[0]));
            for (Iterator<Map.Entry<SimilarState, SimilarState>> ite = track.getRelatedInfo().entrySet().iterator(); ite.hasNext();){
                SimilarState state = ite.next().getValue();
                int comparedTid = Constants.getStateAnoTID(state, tid);
                if ((tid == 8 && comparedTid == 11467) ||
                        (tid == 11467 && comparedTid == 8))
                    System.out.print("");
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
        Global2LocalPoints addPs = new Global2LocalPoints(points);
        sendG2LForAddPs(track, addPs, MBR);
        recalculateTrack(track, MBR, hasNotTopK, addPs, new ArrayList<>(1));
        track.enlargeTuple.f0.bitmap.add(TID);
    }

    private void recalculateOutPointTrack(TrackKeyTID track) {
        try {
            Integer TID = track.trajectory.TID;
            if (track.enlargeTuple.f0 != track.passP.get(0)) track.enlargeTuple.f0.bitmap.remove(TID);
            Rectangle MBR = Constants.getPruningRegion(track.trajectory, 0.0);
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
            Rectangle MBR = Constants.getPruningRegion(track.trajectory, 0.0);
            boolean hasNotTopK = track.topKP.isEmpty();
            //往track经过的节点发送数据
            List<GDataNode> deleteLeafs = (List<GDataNode>) Collections.removeAndGatherElms(track.passP, leaf -> !leaf.region.isIntersection(MBR));
            Global2LocalPoints addPs = new Global2LocalPoints(points);
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
                                  Global2LocalPoints addPs,
                                  List<GDataNode> deleteLeafs) {
        Integer TID = track.trajectory.TID;
        if (track.candidateInfo.size() < Constants.topK + Constants.t){
            List<SimilarState> list = new ArrayList<>(track.getRelatedInfo().keySet());
            java.util.Collections.sort(list);
            List<Integer> newCanDi = new ArrayList<>();
            for (int i = 0; i < Constants.topK + Constants.t && i < list.size(); i++) {
                newCanDi.add(Constants.getStateAnoTID(list.get(i), TID));
            }
            track.candidateInfo.removeAll(newCanDi);
            while (!track.candidateInfo.isEmpty()) track.removeICandidate(1, trackMap);
            track.candidateInfo = newCanDi;
            if (newCanDi.size() < Constants.topK + Constants.t){ //没有足够的候选轨迹
                Rectangle pruneArea = MBR.clone().extendLength(list.get(list.size()-1).distance);
                //筛选出计算阈值的轨迹集合，得出裁剪域
                Set<Integer> selectedTIDs = new HashSet<>();  //阈值计算轨迹集
                while (selectedTIDs.size() < Constants.topK + Constants.t) { //阈值计算轨迹集的元素数要大于Constants.k*Constants.cDTW。
                    selectedTIDs = segmentIndex.getIntersectTIDs(pruneArea);
                    pruneArea.extendMultiple(0.3);   //查询轨迹MBR扩展
                }
                selectedTIDs.removeAll(newCanDi);
                for (Integer selectedTID : selectedTIDs)
                    track.addTrackCandidate(trackMap.get(selectedTID));
            }
        }

        Global2LocalPoints trackPs = Global2LocalPoints.ToG2LPoints(track.trajectory);
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
                    for (GLeafAndBound gb : track.topKP.getList()) {
                        if (deleteLeafs.remove(gb.leaf)) {
                            out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 6, new Global2LocalTID(TID)));
                        } else {
                            out.collect(new Global2LocalElem(gb.leaf.leafID, (byte) 3, trackPs));
                            gb.leaf.bitmap.add(TID);
                        }
                    }
                }
            }
        }else { //有topK扩展节点
            Rectangle pruneArea = DTConstants.recalculateTrackTopK(track, MBR, segmentIndex, trackMap);
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
                    if (oldTopK.remove(gb) && addPs != null) {
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
    }

    /**
     * 轨迹track新添加了一些采样点addPs， 会产生一些新的passP节点，
     * 向这些新增的passP节点和原有的passP节点中发送track的采样点信息。
     */
    private void sendG2LForAddPs(TrackKeyTID track,
                                 Global2LocalPoints addPs,
                                 Rectangle MBR) {
        List<GDataNode> newMBRLeafs = globalTree.getIntersectLeafNodes(MBR);
        Integer TID = track.trajectory.TID;
        Global2LocalPoints trackPs = Global2LocalPoints.ToG2LPoints(track.trajectory);
        track.passP.forEach(dataNode -> out.collect(new Global2LocalElem(dataNode.leafID, (byte) 0, addPs)));
        newMBRLeafs.removeAll(track.passP);
        if (!newMBRLeafs.isEmpty()) {
            if (track.topKP.isEmpty()) {
                for (GDataNode leaf : newMBRLeafs) {
                    out.collect(new Global2LocalElem(leaf.leafID, (byte) 2, trackPs));
                    leaf.bitmap.add(TID);
                }
            } else {
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
                        if (track.topKP.getFirst().bound < track.threshold){ //topK扩展节点变少
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
            out.collect(new Global2LocalElem(gB.leaf.leafID, (byte) 5, new Global2LocalTID(TID)));
        });
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
    }
}
