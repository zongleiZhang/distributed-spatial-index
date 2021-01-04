package com.ada.DTflinkFunction;

import com.ada.QBSTree.RCtree;
import com.ada.common.Arrays;
import com.ada.common.Constants;
import com.ada.geometry.*;
import com.ada.globalTree.GDataNode;
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
                        Collector<Global2LocalElem> out) throws Exception {
        this.out = out;
        RoaringBitmap bitmap = new RoaringBitmap();
        Map<Integer, List<Segment>> inSegMap = new HashMap<>();
        int[][] density = preElements(elements, bitmap, inSegMap);
        tIDsQue.add(bitmap);
        if (density != null){
            densityQue.add(density);
            Arrays.addArrsToArrs(globalTree.density, density, true);
        }

        long logicWinStart = context.window().getEnd() - Constants.windowSize*Constants.logicWindow;
        if (hasInit){
            RoaringBitmap outTIDs = new RoaringBitmap();
            if (densityQue.size() > Constants.logicWindow)  outTIDs = tIDsQue.remove();
            RoaringBitmap inAndOutTIDs = bitmap.clone();
            inAndOutTIDs.and(outTIDs);
            RoaringBitmap inTIDs = bitmap.clone();
            inTIDs.andNot(inAndOutTIDs);
            outTIDs.andNot(inAndOutTIDs);

            //处理只有滑入没有滑出的轨迹
            for (Integer tid : inTIDs) {
                TrackKeyTID track = trackMap.get(tid);
                List<Segment> segments = inSegMap.get(tid);
                if (track.trajectory.elms.size() == segments.size()){ //新的轨迹
                    Rectangle MBR = track.rect.clone();
                    Rectangle pruneArea = MBR.clone().extendLength(Constants.extend);
                    track.rect = DTConstants.newTrackCalculate(track, MBR, pruneArea, segmentIndex, trackMap);
                    globalTree.countPartitions(MBR, track);
                    track.enlargeTuple.f0.bitmap.add(track.trajectory.TID);
                    Global2LocalPoints g2LPoints = Global2LocalPoints.ToG2LPoints(track.trajectory);
                    if (!track.topKP.isEmpty()) {
                        outAddTopKTrackGB(track.topKP.getList(), g2LPoints);
                        pruneIndex.insert(track);
                    }
                    outAddPassTrack(track.passP, g2LPoints);
                    mayBeAnotherTopK(track);
                }else { //已有轨迹
                    updateTrackRelated(inSegMap.get(tid), track);
                    //重新做一次裁剪和距离计算
                    track.sortCandidateInfo();
                    recalculateAddPointTrackTopK(track, segment);
                }
            }

            //整条轨迹滑出窗口的轨迹ID集
            Set<Integer> emptyTIDs = new HashSet<>();

            //有采样点滑出窗口的轨迹（但没有整条滑出），记录其ID和滑出的Segment
            Map<Integer, List<Segment>> outSegMap = new HashMap<>();

            DTConstants.removeSegment(outTIDs, logicWinStart, outSegMap, emptyTIDs, segmentIndex, trackMap);
        }else { //轨迹足够多时，才开始计算，计算之间进行初始化
            if (density != null) globalTree.updateTree();
            if (tIDsQue.size() > Constants.logicWindow){ //窗口完整后才能进行初始化计算
                for (Integer tid : tIDsQue.remove()) trackMap.get(tid).trajectory.removeElem(logicWinStart);
                for (TrackKeyTID track : trackMap.values()) {
                    globalTree.root.getIntersectLeafNodes(track.rect, track.passP);
                    for (GDataNode leaf : track.passP) leaf.bitmap.add(track.trajectory.TID);
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
                    for (TrackKeyTID track : trackMap.values()) track.passP.clear();
                    for (GDataNode leaf : globalTree.getAllLeafs()) leaf.bitmap.clear();
                }
            }
        }


        // 调整Global Index, 然后将调整结果同步到相关的Local Index中。
        if (density != null){
            densityQue.add(density);
            Arrays.addArrsToArrs(globalTree.density, density, true);
            if (densityQue.size() > Constants.logicWindow / Constants.balanceFre) {
                int[][] removed = densityQue.remove();
                Arrays.addArrsToArrs(globalTree.density, removed, false);
            }

            //调整Global Index
            Map<GNode, GNode> nodeMap = globalTree.updateTree();

            //Global Index发生了调整，通知Local Index迁移数据，重建索引。
            if (!nodeMap.isEmpty() && subTask == 0)
                adjustLocalTasksRegion(nodeMap);
        }
    }

    /**
     * 已有轨迹track有新的轨迹段seg到达，更新缓存中其相关的中间结果
     */
    private void updateTrackRelated(List<Segment> segments, TrackKeyTID track) {
        List<Integer> removeRI = new ArrayList<>();
        Integer TID = track.trajectory.TID;
        //更新新采样点所属的轨迹与相关轨迹的距离
        for (SimilarState state : track.getRelatedInfo().values()) {
            int comparedTID = Constants.getStateAnoTID(state, TID);
            TrackKeyTID comparedTrack = trackMap.get(comparedTID);
            int oldIndex = comparedTrack.candidateInfo.indexOf(TID);
            List<TrackPoint> list = Segment.segmentsToPoints(segments);
            list.remove(0);
            Constants.incrementHausdorff(list, comparedTrack.trajectory, state);
            if (oldIndex != -1){ //track是comparedTrack的候选轨迹
                if (comparedTrack.rect.isInternal(list)){
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
                Tuple2<Boolean, Rectangle> tuple2 = DTConstants.enlargePrune(track, dis, notRemove, segmentIndex, trackMap);
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
            for (TrackKeyTID comparedTrack : list) Constants.addTrackCandidate(track, comparedTrack);
            track.sortCandidateInfo();
            track.threshold = track.getKCanDistance(Constants.t + Constants.topK).distance;
            Rectangle MBR = track.rect.clone();
            track.rect = MBR.clone().extendLength(track.threshold);
            globalTree.countPartitions(MBR, track);
            track.enlargeTuple.f0.bitmap.add(TID);
            Global2LocalPoints g2LPoints = Global2LocalPoints.ToG2LPoints(track.trajectory);
            if (!track.topKP.isEmpty()) {
                outAddTopKTrackGB(track.topKP.getList(), g2LPoints);
                pruneIndex.insert(track);
            }
            for (int i = Constants.t + Constants.topK; i < track.candidateInfo.size(); i++) {
                int comparedTID = track.candidateInfo.get(i);
                TrackKeyTID comparedTrack = trackMap.get(comparedTID);
                if (comparedTrack.outSideRectangle(track.rect)){
                    track.removeTIDCandidate(comparedTrack);
                    i--;
                }
            }
            outAddPassTrack(track.passP, g2LPoints);
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
     * 对窗口输入数据elements进行预处理，包括识别并统计出密度网格、将轨迹
     * 点按照轨迹ID分类到Map中、统计所有的轨迹ID到RoaringBitmap中、将轨
     * 迹点添加到轨迹中。
     */
    private int[][] preElements(Iterable<Density2GlobalElem> elements,
                                RoaringBitmap bitmap,
                                Map<Integer , List<Segment>> inSegMap) {
        List<Density> densities = new ArrayList<>(Constants.globalPartition);
        Map<Integer, List<TrackPoint>> map = new HashMap<>();
        for (Density2GlobalElem element : elements) {
            if (element instanceof TrackPoint){
                TrackPoint point = (TrackPoint) element;
                map.computeIfAbsent(point.TID, tid -> new ArrayList<>(4)).add(point);
            }else {
                densities.add((Density) element);
            }
        }
        int[][] density = null;
        if (!densities.isEmpty()){
            density = new int[Constants.gridDensity+1][Constants.gridDensity+1];
            for (Density elem : densities) Arrays.addArrsToArrs(density, elem.grids, true);
        }

        for (Map.Entry<Integer, List<TrackPoint>> entry : map.entrySet()) {
            TrackKeyTID track = trackMap.get(entry.getKey());
            List<Segment> segments;
            if (track == null){ //trackMap 中没有的轨迹点
                TrackPoint prePoint = singlePointMap.remove(entry.getKey());
                if (prePoint == null){ //新的轨迹点，且只有一个点。
                    if (entry.getValue().size() == 1){
                        singlePointMap.put(entry.getKey(), entry.getValue().get(0));
                        continue;
                    }else { //新的轨迹点产生
                        segments = Segment.pointsToSegments(entry.getValue());
                        Rectangle rect = Rectangle.getUnionRectangle(com.ada.common.collections.Collections.changeCollectionElem(segments, seg -> seg.rect).toArray(new Rectangle[]));
                        track = new TrackKeyTID(null,
                                rect.getCenter().data,
                                rect,
                                new ArrayDeque<>(segments),
                                entry.getKey(),
                                new ArrayList<>(),
                                new HashMap<>());
                        trackMap.put(entry.getKey(), track);
                    }
                }
            }else { //已有的轨迹
                entry.getValue().add(0, track.trajectory.elms.getLast().p2);
                segments = Segment.pointsToSegments(entry.getValue());
                track.trajectory.addSegments(segments);
            }
            for (Segment segment : segments) segmentIndex.insert(segment);
            bitmap.add(entry.getKey());
            inSegMap.put(entry.getKey(), segments);
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
