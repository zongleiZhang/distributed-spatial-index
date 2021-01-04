package com.ada.DTflinkFunction;

import com.ada.geometry.GridPoint;
import com.ada.geometry.GridRectangle;
import com.ada.QBSTree.DualRootTree;
import com.ada.QBSTree.RCtree;
import com.ada.common.Constants;
import com.ada.geometry.*;
import com.ada.model.globalToLocal.Global2LocalElem;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;

public class HausdorffDivideMF extends RichFlatMapFunction<TwoThreeData, String> {
    private Map<Long, Integer> waterMap;
    private long curWater;
    private long startWindow;
    private Map<Long, RoaringBitmap> startWinTIDs;
    private RoaringBitmap TIDs;
    private LinkedList<Global2LocalElem> trackInfoQueue;
    private Comparator<Global2LocalElem> comparator;
    private Tuple2<Boolean, Rectangle> adjInfo;

    private Map<Integer, TrackHauOne> passTrackMap;
    private Map<Integer, TrackHauOne> topKTrackMap;
    private DualRootTree<Segment> pointIndex;
    private RCtree<TrackHauOne> pruneIndex;

    @Override
    public void flatMap(TwoThreeData value, Collector<String> out) throws Exception {
        if (value instanceof Global2LocalElem){ //轨迹信息
            DTConstants.addOnePointQueue(trackInfoQueue, (Global2LocalElem) value, comparator);
        }else if (value instanceof TwoThreeWater){ //水印
            Long water = ((TwoThreeWater) value).water;
            int wNum = waterMap.get(water);
            wNum++;
            if (wNum == Constants.globalPartition){ //水印上升
                waterMap.remove(water);
                curWater = water;
                List<Global2LocalElem> trackInfo = removePointQueue(curWater);
                long minus = curWater - startWindow;
                if ( minus >= Constants.windowSize ){ //窗口发生滑动
                    startWindow += ((int) (minus/Constants.windowSize))*Constants.windowSize;
                    TIDs = new RoaringBitmap();
                    startWinTIDs.put(startWindow, TIDs);
                    long logicStart = curWater - Constants.windowSize*Constants.logicWindow;
                    RoaringBitmap head = startWinTIDs.remove(logicStart);
                    if (adjInfo.f0){ //索引区域发生变化
                        adjInfo.f0 = false;
                        if (adjInfo.f1 == null){ //废弃本节点
                            processPoint(trackInfo);
                            windowSlide(head, logicStart);
                            open(null);
                        }else { //调整本节点
                            trackInfo.sort(Comparator.comparingInt(o -> o.flag));
                            Map<Integer,List<Message>> adjRegionInfo = new HashMap<>();
                            for (int i = 6; i < 12; i++)
                                adjRegionInfo.put(i, new ArrayList<>());
                            for (int i = trackInfo.size() - 1; i > -1; i--) {
                                Global2LocalElem info = trackInfo.remove(i);
                                if (info.flag > 5) {
                                    adjRegionInfo.get((int)info.flag).add(info.message);
                                }else {
                                    trackInfo.add(info);
                                    break;
                                }
                            }
                            processPoint(trackInfo);
                            windowSlide(head, startWindow);
                            adjustRegion(adjInfo.f1,adjRegionInfo);
                        }
                    }else { //索引区域不变
                        processPoint(trackInfo);
                        windowSlide(head, logicStart);
                    }

                }else { //窗口没有滑动
                    processPoint(trackInfo);
                }
            }else {
                waterMap.replace(water, wNum);
            }
        }else { //节点调整信息
            adjInfo.f0 = true;
            adjInfo.f1 = ((TwoThreeAdjRegion) value).region;
        }
    }

    private void adjustRegion(Rectangle newRectangle,
                              Map<Integer, List<Message>> adjRegionInfo) throws Exception {
        if (adjRegionInfo.get(8).size() > (passTrackMap.size()*7/10.0)){ //删除的轨迹太多重新构建
            for (Message info : adjRegionInfo.get(6)) { //6： (调整负责区域)经过轨迹改为topK轨迹   (TrackPoint)
                int TID = ((TrackPoint) info).TID;
                topKTrackMap.put(TID,passTrackMap.remove(TID));
            }

            for (Message info : adjRegionInfo.get(7)) { //7： (调整负责区域)topK轨迹改为经过轨迹   (TrackPoint)
                int TID = ((TrackPoint) info).TID;
                passTrackMap.put(TID,topKTrackMap.remove(TID));
            }

            for (Message info : adjRegionInfo.get(8))  //8： (调整负责区域)删除经过轨迹   (TrackPoint)
                passTrackMap.remove(((TrackPoint) info).TID);

            for (Message info : adjRegionInfo.get(9))  //9： (调整负责区域)删除topK轨迹   (TrackPoint)
                topKTrackMap.remove(((TrackPoint) info).TID);

            for (Message info : adjRegionInfo.get(10)) { //10：(调整负责区域)新增经过轨迹   (TrackMessage)
                TrackHauOne track = toTrackHauOne((TrackMessage) info);
                passTrackMap.put(track.trajectory.TID, track);
            }

            for (Message info : adjRegionInfo.get(11)) { //11：(调整负责区域)新增topK轨迹   (TrackMessage)
                TrackHauOne track = toTrackHauOne((TrackMessage) info);
                topKTrackMap.put(track.trajectory.TID, track);
            }

            for (TrackHauOne track : passTrackMap.values())
                track.clear();

            for (TrackHauOne track : topKTrackMap.values())
                track.clear();

            for (RoaringBitmap bitmap : startWinTIDs.values())
                bitmap.clear();

            List<Segment> innerElems = new ArrayList<>();
            List<Segment> outerElems = new ArrayList<>();
            Rectangle innerMBR = null;
            Rectangle outerMBR = null;
            for (TrackHauOne track : passTrackMap.values()) {
                int TID = track.trajectory.TID;
                long startWin = ((int) (track.trajectory.elms.getFirst().p1.timestamp/Constants.windowSize)) * Constants.windowSize;
                startWinTIDs.get(startWin).add(TID);
                for (Segment segment : track.trajectory.elms) {
                    startWin = ((int) (segment.p2.timestamp/Constants.windowSize)) * Constants.windowSize;
                    startWinTIDs.get(startWin).add(TID);
                    if (newRectangle.isInternal(segment)) {
                        innerElems.add(segment);
                        if (innerMBR == null)
                            innerMBR = segment.rect.clone();
                        else
                            innerMBR = innerMBR.getUnionRectangle(segment.rect);
                    }else {
                        outerElems.add(segment);
                        if (outerMBR == null)
                            outerMBR = segment.rect.clone();
                        else
                            outerMBR = outerMBR.getUnionRectangle(segment.rect);
                    }
                }
            }
            pointIndex.rebuildRoot(newRectangle, innerElems, outerElems, innerMBR, outerMBR);

            for (TrackHauOne track : passTrackMap.values())
                newTrackCalculate(track);

            for (TrackHauOne track : topKTrackMap.values())
                newTrackCalculate(track);
        }else { //删除的轨迹不多，在原有数据的基础上重建
            //topK结果可能发生变化的轨迹TID
            RoaringBitmap pruneChangeTIDs = new RoaringBitmap();

            for (Message info : adjRegionInfo.get(9))   //9： (调整负责区域)删除topK轨迹   (TrackPoint)
                removeTopKTrack(topKTrackMap.remove(((TrackPoint) info).TID));

            for (Message info : adjRegionInfo.get(8)) {  //8： (调整负责区域)删除经过轨迹   (TrackPoint)
                int TID = ((TrackPoint) info).TID;
                pruneChangeTIDs.remove(TID);
                TrackHauOne track = passTrackMap.remove(TID);
                pruneIndex.delete(track);
                track.getRelatedInfo().forEach((state, state1) -> {
                    int comparedTid = Constants.getStateAnoTID(state, TID);
                    TrackHauOne comparedTrack = passTrackMap.get(comparedTid);
                    if (comparedTrack == null)
                        comparedTrack = topKTrackMap.get(comparedTid);
                    int index = comparedTrack.candidateInfo.indexOf(TID);
                    comparedTrack.candidateInfo.remove(TID);
                    comparedTrack.removeRelatedInfo(state);
                    if (index != -1)
                        pruneChangeTIDs.add(comparedTid);
                });
                for (Segment elem : track.trajectory.elms)
                    pointIndex.delete(elem);
            }

            for (Message info : adjRegionInfo.get(6)) { //6： (调整负责区域)经过轨迹改为topK轨迹   (TrackPoint)
                int TID = ((TrackPoint) info).TID;
                TrackHauOne track = passTrackMap.remove(TID);
                topKTrackMap.put(TID, track);
                for (Iterator<Map.Entry<SimilarState, SimilarState>> ite = track.getRelatedInfo().entrySet().iterator(); ite.hasNext();){
                    SimilarState state = ite.next().getKey();
                    int comparedTid = Constants.getStateAnoTID(state, TID);
                    TrackHauOne comparedTrack = passTrackMap.get(comparedTid);
                    if (comparedTrack == null)
                        comparedTrack = topKTrackMap.get(comparedTid);
                    int index = comparedTrack.candidateInfo.indexOf(TID);
                    if (index != -1){//comparedTrack的候选轨迹里有track
                        if (!track.candidateInfo.contains(comparedTid)) {
                            comparedTrack.removeRelatedInfo(state);
                            ite.remove();
                        }
                        comparedTrack.candidateInfo.remove(TID);
                        pruneChangeTIDs.add(comparedTid);
                    }
                }
                for (Segment elem : track.trajectory.elms)
                    pointIndex.delete(elem);
            }

            for (Message info : adjRegionInfo.get(7)) { //7： (调整负责区域)topK轨迹改为经过轨迹   (TrackPoint)
                int TID = ((TrackPoint) info).TID;
                TrackHauOne track = topKTrackMap.remove(TID);
                passTrackMap.put(TID, track);
                for (Segment elem : track.trajectory.elms)
                    pointIndex.insert(elem);
                Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
                rebuildMeyBeAnoTopK(pruneChangeTIDs, track, MBR);
            }


            for (Message info : adjRegionInfo.get(10)) { //10：(调整负责区域)新增经过轨迹   (TrackMessage)
                TrackHauOne track = toTrackHauOne((TrackMessage) info);
                passTrackMap.put(track.trajectory.TID, track);
                for (Segment elem : track.trajectory.elms)
                    pointIndex.insert(elem);
                Rectangle MBR = track.rect.clone();
                track.rect = DTConstants.newTrackCalculate(track, MBR, MBR.clone(), pointIndex, passTrackMap);
                pruneIndex.insert(track);
                rebuildMeyBeAnoTopK(pruneChangeTIDs, track, MBR);
            }

            for (Message info : adjRegionInfo.get(11)) { //11：(调整负责区域)新增topK轨迹   (TrackMessage)
                TrackHauOne track = toTrackHauOne((TrackMessage) info);
                topKTrackMap.put(track.trajectory.TID, track);
                Rectangle MBR = track.rect.clone();
                track.rect = DTConstants.newTrackCalculate(track, MBR, MBR.clone(), pointIndex, passTrackMap);
                pruneIndex.insert(track);
            }

            for (Integer TID : pruneChangeTIDs) {
                TrackHauOne track = passTrackMap.get(TID);
                if (track == null)
                    track = topKTrackMap.get(TID);
                if (track.candidateInfo.size() < Constants.topK){ //候选轨迹太少了
                    DTConstants.dealCandidateSmall(new HashMap<>(), TID, track, passTrackMap, topKTrackMap, pruneIndex, pointIndex);
                }else { //候选轨迹足够
                    DTConstants.changeThreshold(track, -1, null, pruneIndex, pointIndex,passTrackMap);
                }
            }
        }
    }

    private void rebuildMeyBeAnoTopK(RoaringBitmap pruneChangeTIDs, TrackHauOne track, Rectangle MBR) {
        int TID = track.trajectory.TID;
        List<Integer> list = pruneIndex.trackInternal(MBR);
        list.remove(TID);
        for (Integer comparedTid : list) {
            TrackHauOne comparedTrack = passTrackMap.get(comparedTid);
            if (comparedTrack == null)
                comparedTrack = topKTrackMap.get(comparedTid);
            if (!comparedTrack.candidateInfo.contains(TID)){
                Constants.addTrackCandidate(comparedTrack, track);
                comparedTrack.updateCandidateInfo(TID);
                pruneChangeTIDs.add(comparedTid);
            }
        }
    }


    private void newTrackCalculate(TrackHauOne track) {
        try {
            Integer TID = track.trajectory.TID;
            Rectangle MBR = track.rect.clone();
            Rectangle pruneArea = MBR.clone().extendLength(Constants.extend);
            //筛选出计算阈值的轨迹集合，得出裁剪域
            Set<Integer> selectedTIDs = new HashSet<>();  //阈值计算轨迹集
            while (selectedTIDs.size() < Constants.topK * Constants.KNum) { //阈值计算轨迹集的元素数要大于Constants.k*Constants.cDTW。
                selectedTIDs = pointIndex.getRegionInternalTIDs(pruneArea);
                pruneArea.extendMultiple(0.3);   //查询轨迹MBR扩展
            }
            selectedTIDs.remove(TID);
            Constants.cutTIDs(selectedTIDs);
            List<SimilarState> result = new ArrayList<>();
            for (Integer comparedTid : selectedTIDs) {
                TrackHauOne comparedTrack = passTrackMap.get(comparedTid);
                SimilarState state = comparedTrack.getSimilarState(TID);
                if (state == null)
                    Constants.getHausdorff(track.trajectory, comparedTrack.trajectory);
                result.add(state);
            }
            Collections.sort(result);
            SimilarState thresholdState = result.get(Constants.topK + Constants.t - 1);
            double threshold  = thresholdState.distance;
            pruneArea = MBR.clone().extendLength(threshold);

            //用裁剪域筛选出候选轨迹集，计算距离并排序
            Set<Integer> needCompareTIDS = pointIndex.getRegionInternalTIDs(pruneArea);
            needCompareTIDS.remove(TID);
            List<SimilarState> needCompareState = new ArrayList<>();
            for (Integer compareTid : needCompareTIDS) {
                TrackHauOne comparedTrack = passTrackMap.get(compareTid);
                SimilarState state = comparedTrack.getSimilarState(TID);
                if (state == null){
                    SimilarState tmpState = new SimilarState(TID, compareTid, null, null);
                    int index = result.indexOf(tmpState);
                    if (index == -1){
                        tmpState.convert();
                        index = result.indexOf(tmpState);
                        if (index == -1)
                            state = Constants.getHausdorff(track.trajectory, comparedTrack.trajectory);
                        else
                            state = result.get(index);
                    }else {
                        state = result.get(index);
                    }
                }
                needCompareState.add(state);
            }
            Collections.sort(needCompareState);

            //紧缩裁剪区以查找需要保留相似度计算中间结果的轨迹
            if (needCompareState.indexOf(thresholdState) > Constants.topK + Constants.t*2 - 1) {
                threshold = needCompareState.get(Constants.topK + Constants.t - 1).distance;
                pruneArea = MBR.clone().extendLength(threshold);
                for (int i = 0; i < Constants.topK + Constants.t; i++) {
                    SimilarState state = needCompareState.get(i);
                    int comparedTID = Constants.getStateAnoTID(state, TID);
                    track.candidateInfo.add(comparedTID);
                    track.putRelatedInfo(state);
                    passTrackMap.get(comparedTID).putRelatedInfo(state);
                }
                for (int i = Constants.topK + Constants.t; i < needCompareState.size(); i++) {
                    SimilarState state = needCompareState.get(i);
                    int comparedTID = Constants.getStateAnoTID(state, TID);
                    TrackHauOne comparedTrack = passTrackMap.get(comparedTID);
                    if (!comparedTrack.outSideRectangle(pruneArea)){
                        track.candidateInfo.add(comparedTID);
                        track.putRelatedInfo(state);
                        comparedTrack.putRelatedInfo(state);
                    }
                }
            } else {
                for (SimilarState state : needCompareState) {
                    int comparedTID = Constants.getStateAnoTID(state, TID);
                    track.candidateInfo.add(comparedTID);
                    track.putRelatedInfo(state);
                    passTrackMap.get(comparedTID).putRelatedInfo(state);
                }
            }
            track.threshold = threshold;
            track.rect = pruneArea;
            pruneIndex.insert(track);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 窗口滑动逻辑
     * @param head 滑出的tuple
     * @param logicStartWin 新窗口的开始时间
     */
    private void windowSlide(RoaringBitmap head, long logicStartWin) {

        //整条轨迹滑出窗口的轨迹ID集
        Set<Integer> emptyTIDs = new HashSet<>();

        //有采样点滑出窗口的轨迹（但没有整条滑出），记录其ID和滑出的Segment
        Map<Integer, List<Segment>> removeElemMap = new HashMap<>();

        for (Integer tid : head) {
            TrackHauOne track = passTrackMap.get(tid);
            if (track == null){
                track = topKTrackMap.get(tid);
                if (track != null) {
                    //topK处理
                    List<Segment> timeElems = track.trajectory.removeElem(logicStartWin);
                    if (timeElems.size() != 0) {
                        if (track.trajectory.elms.size() == 0)
                            removeTopKTrack(track);
                        else
                            removeElemMap.put(tid, timeElems);
                    }
                }
            }else  {
                List<Segment> timeElems = track.trajectory.removeElem(logicStartWin);
                if (timeElems.size() != 0) {
                    for (Segment segment : timeElems)
                        pointIndex.delete(segment);
                    if (track.trajectory.elms.size() == 0)
                        emptyTIDs.add(tid);
                    else
                        removeElemMap.put(tid, timeElems);
                }
            }
        }

        //记录无采样点滑出，但其topK结果可能发生变化的轨迹TID
        Set<Integer> pruneChangeTIDs = new HashSet<>();

        //记录轨迹滑出导致其候选轨迹集小于K的轨迹ID集
        Set<Integer> canSmallTIDs = new HashSet<>();

        //处理整条轨迹滑出窗口的轨迹
        for (Integer tid : emptyTIDs)
            DTConstants.dealAllSlideOutTracks(passTrackMap.remove(tid), head, pruneChangeTIDs, emptyTIDs, canSmallTIDs, passTrackMap, topKTrackMap,pruneIndex);

        //处理整条轨迹未完全滑出窗口的轨迹
        DTConstants.trackSlideOut(removeElemMap, pruneChangeTIDs, canSmallTIDs,passTrackMap,topKTrackMap,pruneIndex,pointIndex);

        pruneChangeTIDs.removeAll(canSmallTIDs);
        for (Integer tid : pruneChangeTIDs) {
            if (tid == 2563)
                System.out.print("");
            DTConstants.changeThreshold(passTrackMap.get(tid), -1, null, pruneIndex, pointIndex, passTrackMap);
        }
        for (Integer tid : canSmallTIDs) {
            if (tid == 2563)
                System.out.print("");
            DTConstants.dealCandidateSmall(removeElemMap, tid, null, passTrackMap,topKTrackMap,pruneIndex,pointIndex);
        }

        for (Integer tid : removeElemMap.keySet()) {
            TrackHauOne track = passTrackMap.get(tid);
            if (track != null)
                DTConstants.mayBeAnotherTopK(track, pruneIndex, passTrackMap, topKTrackMap);
        }
    }



    private void processPoint(List<Global2LocalElem> trackInfo){
        try {
            for (Global2LocalElem info : trackInfo) {
                if (info.flag == 0 || info.flag == 1) { //0:添加经过点;  1:添加topK点
                    TrackPoint point = (TrackPoint) info.message;
                    int TID = point.TID;
                    TIDs.add(TID);
                    TrackHauOne track;
                    if (info.flag == 0)
                        track = passTrackMap.get(TID);
                    else
                        track = topKTrackMap.get(TID);
                    Segment segment = new Segment(track.trajectory.elms.getLast().p2, point);
                    track.trajectory.addElem(segment);
                    Rectangle MBR = track.rect.clone().extendLength(-track.threshold);
                    MBR = MBR.getUnionRectangle(segment.rect);
                    if (info.flag == 0) {
                        pointIndex.insert(segment);
                        DTConstants.updateTrackRelated(segment, track, passTrackMap, topKTrackMap, pruneIndex, pointIndex);
                    }else {
                        //更新新采样点所属的轨迹与相关轨迹的距离
                        for (SimilarState state : track.getRelatedInfo().values()) {
                            int comparedTid = Constants.getStateAnoTID(state, TID);
                            TrackHauOne comparedTrack = passTrackMap.get(comparedTid);
                            Constants.incrementHausdorff(Collections.singletonList(segment.p2), comparedTrack.trajectory, state);
                        }
                    }
                    //重新做一次裁剪和距离计算
                    track.sortCandidateInfo();
                    Rectangle pruneArea = DTConstants.recalculateTrackTopK(track, MBR, pointIndex, passTrackMap);
                    pruneIndex.alterELem(track, pruneArea);
                }else if (info.flag == 2) { //新增经过轨迹   (TrackMessage)
                    TrackHauOne track = toTrackHauOne((TrackMessage) info.message);
                    for (Segment elem : track.trajectory.elms)
                        pointIndex.insert(elem);
                    passTrackMap.put(track.trajectory.TID, track);
                    if (passTrackMap.size() > Constants.topK * Constants.KNum) {
                        Rectangle MBR = track.rect.clone();
                        track.rect = DTConstants.newTrackCalculate(track, MBR, MBR.clone(), pointIndex, passTrackMap);
                        pruneIndex.insert(track);
                        DTConstants.mayBeAnotherTopK(track, pruneIndex, passTrackMap, topKTrackMap);
                    } else {
                        if (passTrackMap.size() == Constants.topK * Constants.KNum) {
                            DTConstants.initCalculate(new ArrayList<>(passTrackMap.values()),
                                    new ArrayList<>(passTrackMap.values()), pruneIndex);
                            DTConstants.initCalculate(new ArrayList<>(topKTrackMap.values()),
                                    new ArrayList<>(passTrackMap.values()), pruneIndex);
                        }
                    }
                } else if (info.flag == 3) { //新增topK轨迹   (TrackMessage)
                    TrackHauOne track = toTrackHauOne((TrackMessage) info.message);
                    topKTrackMap.put(track.trajectory.TID, track);
                    if (passTrackMap.size() > Constants.topK * Constants.KNum) {
                        Rectangle MBR = track.rect.clone();
                        track.rect = DTConstants.newTrackCalculate(track, MBR, MBR.clone(), pointIndex, passTrackMap);
                        pruneIndex.insert(track);
                    }
                } else if (info.flag == 4) { //删除经过轨迹   (TrackPoint)
                    removePassTrack(passTrackMap.remove(((TrackPoint) info.message).TID));
                } else { //删除topK轨迹   (TrackPoint)
                    removeTopKTrack(topKTrackMap.remove(((TrackPoint) info.message).TID));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void removePassTrack(TrackHauOne track){
        Integer TID = track.trajectory.TID;
        pruneIndex.delete(track);
        track.getRelatedInfo().forEach((state, state1) -> {
            int comparedTid = Constants.getStateAnoTID(state, TID);
            TrackHauOne comparedTrack = passTrackMap.get(comparedTid);
            if (comparedTrack == null)
                comparedTrack = topKTrackMap.get(comparedTid);
            int index = comparedTrack.candidateInfo.indexOf(TID);
            comparedTrack.candidateInfo.remove(TID);
            comparedTrack.removeRelatedInfo(state);
            if (index != -1)
                DTConstants.changeThreshold(track, -1, null,pruneIndex, pointIndex, passTrackMap);
        });
        for (Segment elem : track.trajectory.elms)
            pointIndex.delete(elem);
    }

    private void removeTopKTrack(TrackHauOne track){
        int TID = track.trajectory.TID;
        pruneIndex.delete(track);
        track.getRelatedInfo().forEach((state, state1) -> passTrackMap.get(Constants.getStateAnoTID(state, TID)).removeRelatedInfo(state));
    }


    private TrackHauOne toTrackHauOne(TrackMessage trackMessage){
        LinkedList<Segment> elems = new LinkedList<>();
        TrackPoint prePoint =  trackMessage.elems.get(0);
        TrackPoint curPoint;
        long startWin = ((int) (prePoint.timestamp/Constants.windowSize)) * Constants.windowSize;
        startWinTIDs.get(startWin).add(prePoint.TID);
        for (int i = 1; i < trackMessage.elems.size(); i++) {
            curPoint = trackMessage.elems.get(i);
            elems.add(new Segment(prePoint, curPoint));
            prePoint = curPoint;
            startWin = ((int) (prePoint.timestamp/Constants.windowSize)) * Constants.windowSize;
            startWinTIDs.get(startWin).add(prePoint.TID);
        }
        TrackHauOne track = new TrackHauOne();
        track.trajectory = new Trajectory<>(elems, elems.getFirst().getTID());
        track.rect = Constants.getPruningRegion(track.trajectory, 0.0);
        track.candidateInfo = new ArrayList<>();
        track.setRelatedInfo(new HashMap<>());
        track.data = track.rect.getCenter().data;
        return track;
    }

    private List<Global2LocalElem> removePointQueue(long timeStamp){
        List<Global2LocalElem> list = new ArrayList<>();
        while (trackInfoQueue.getLast().message.getTimeStamp() < timeStamp && !trackInfoQueue.isEmpty())
            list.add(trackInfoQueue.removeLast());
        return list;
    }

    @Override
    public void open(Configuration parameters) {
        curWater = 0L;
        startWindow = 0L;
        startWinTIDs = new HashMap<>();
        waterMap = new HashMap<>();
        trackInfoQueue = new LinkedList<>();
        comparator = Comparator.comparingLong(o -> o.message.getTimeStamp());
        adjInfo = new Tuple2<>(false, null);
        passTrackMap = new HashMap<>();
        topKTrackMap = new HashMap<>();
        Rectangle rectangle;
        switch (getRuntimeContext().getIndexOfThisSubtask()){
            case 0:
                rectangle = (new GridRectangle(new GridPoint(0,0), new GridPoint(200, 200))).toRectangle();
                pointIndex = new DualRootTree<>(8,1,17, rectangle, Constants.globalRegion, true);
                pruneIndex  = new RCtree<>(8,1,17, rectangle,0, false);
                break;
            case 1:
                rectangle = (new GridRectangle(new GridPoint(0,201), new GridPoint(200, 511))).toRectangle();
                pointIndex = new DualRootTree<>(8,1,17, rectangle,Constants.globalRegion, true);
                pruneIndex  = new RCtree<>(8,1,17, rectangle,0, false);
                break;
            case 2:
                rectangle = (new GridRectangle(new GridPoint(201,0), new GridPoint(511, 200))).toRectangle();
                pointIndex = new DualRootTree<>(8,1,17, rectangle,Constants.globalRegion, true);
                pruneIndex  = new RCtree<>(8,1,17, rectangle,0, false);
                break;
            case 3:
                rectangle = (new GridRectangle(new GridPoint(201,201), new GridPoint(511, 511))).toRectangle();
                pointIndex = new DualRootTree<>(8,1,17, rectangle,Constants.globalRegion, true);
                pruneIndex  = new RCtree<>(8,1,17, rectangle,0, false);
                break;
            default:
                break;
        }
    }
}
