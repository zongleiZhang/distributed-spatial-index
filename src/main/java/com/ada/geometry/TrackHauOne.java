package com.ada.geometry;

import com.ada.QBSTree.RCDataNode;
import com.ada.QBSTree.RectElem;
import com.ada.common.Constants;

import java.io.Serializable;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TrackHauOne extends RectElem implements Serializable {
    public Trajectory<Segment> trajectory;
    public List<Integer> candidateInfo;
    private Map<SimilarState, SimilarState> relatedInfo;
    public double threshold;

    public TrackHauOne(){}

    public TrackHauOne(RCDataNode leaf,
                       double[] data,
                       Rectangle rect,
                       LinkedList<Segment> elms,
                       int TID,
                       List<Integer> candidateInfo,
                       Map<SimilarState, SimilarState> relatedInfo) {
        super(leaf, data, rect);
        trajectory = new Trajectory<>(elms, TID);
        this.candidateInfo = candidateInfo;
        this.relatedInfo = relatedInfo;
    }

    public Map<SimilarState, SimilarState> getRelatedInfo() {
        return relatedInfo;
    }

    public void setRelatedInfo(Map<SimilarState, SimilarState> relatedInfo) {
        this.relatedInfo = relatedInfo;
    }

    public SimilarState putRelatedInfo(SimilarState state){
        SimilarState oldState = relatedInfo.get(state);
        if (oldState == null){
            state.convert();
            oldState = relatedInfo.get(state);
            state.convert();
        }
        if (oldState != null) {
            if (!Constants.isEqual(oldState.distance, state.distance))
                System.out.print("");
            return oldState;
//            relatedInfo.remove(state);
        }
        relatedInfo.put(state,state);
        return state;
    }

    public void removeRelatedInfo(SimilarState state){
        relatedInfo.remove(state);
    }

    public void removeRelatedInfo(Integer tid){
        relatedInfo.remove(getSimilarState(tid));
    }

    public void clearRelatedInfo(){
        relatedInfo.clear();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TrackHauOne)) return false;
        TrackHauOne that = (TrackHauOne) o;
        return that.trajectory.equals(trajectory);
    }

    @Override
    public int hashCode() {
        return trajectory.hashCode();
    }

    public SimilarState getSimilarState(int tid2){
        SimilarState tmp = new SimilarState(trajectory.TID, tid2, null, null);
        SimilarState res = relatedInfo.get(tmp);
        if (res == null){
            return relatedInfo.get(tmp.convert());
        }else {
            return res;
        }
    }

    public void sortCandidateInfo(){
        candidateInfo.sort(Comparator.comparing(this::getSimilarState));
    }

    public <T extends TrackHauOne> void removeICandidate(int i, Map<Integer, T> map){
        Integer comparedTID = candidateInfo.remove(i-1);
        T t = map.get(comparedTID);
        if (!t.candidateInfo.contains(trajectory.TID)){
            t.removeRelatedInfo(trajectory.TID);
            removeRelatedInfo(comparedTID);
        }
    }

    public <T extends TrackHauOne> void removeTIDCandidate(Integer comparedTID, Map<Integer, T> map){
        candidateInfo.remove(comparedTID);
        T t = map.get(comparedTID);
        if (!t.candidateInfo.contains(trajectory.TID)){
            t.removeRelatedInfo(trajectory.TID);
            removeRelatedInfo(comparedTID);
        }
    }

    public <T extends TrackHauOne> void removeTIDCandidate(T comparedTrack){
        Integer comparedTID = comparedTrack.trajectory.TID;
        candidateInfo.remove(comparedTID);
        if (!comparedTrack.candidateInfo.contains(trajectory.TID)){
            comparedTrack.removeRelatedInfo(trajectory.TID);
            removeRelatedInfo(comparedTID);
        }
    }

    /**
     * 返回与第k个候选归轨迹的距离
     */
    public SimilarState getKCanDistance(int k){
        if (candidateInfo.size() >= k){
            return getSimilarState(candidateInfo.get(k-1));
        }else if (candidateInfo.size() == 1){
            return getSimilarState(candidateInfo.get(0));
        }else{
            return getSimilarState(candidateInfo.get(candidateInfo.size()-1));
        }
    }

    /**
     * 本轨迹与一条候选轨迹comparedTid的相似度发生变化，更新候选轨迹集的顺序。
     * 返回排序后comparedTid所在的位置。
     */
    public int updateCandidateInfo(int comparedTid) {
        int index = candidateInfo.indexOf(comparedTid);
        SimilarState state2 = getSimilarState(comparedTid);
        int comparedTid1, comparedTid3;
        SimilarState state1, state3;
        if (index == 0) {
            return moveBackward(comparedTid, index, state2);
        }else if (index ==  candidateInfo.size()-1) {
            return moveForward(comparedTid, index, state2);
        }else {
            comparedTid1 = candidateInfo.get(index - 1);
            state1 = getSimilarState(comparedTid1);
            comparedTid3 = candidateInfo.get(index + 1);
            state3 = getSimilarState(comparedTid3);
            if (Double.compare(state2.distance, state1.distance) < 0)
                return moveForward(comparedTid, index, state2);
            if (Double.compare(state2.distance, state3.distance) > 0)
                return moveBackward(comparedTid, index, state2);
            return index;
        }
    }

    private int moveForward(int comparedTid, int index, SimilarState state2) {
        int comparedTid1;
        SimilarState state1;
        while (index != 0){
            comparedTid1 = candidateInfo.get(index - 1);
            state1 = getSimilarState(comparedTid1);
            if (Double.compare(state2.distance, state1.distance) < 0) {
                candidateInfo.set(index, comparedTid1);
            }else {
                candidateInfo.set(index, comparedTid);
                return index;
            }
            index--;
        }
        candidateInfo.set(index, comparedTid);
        return index;
    }

    private int moveBackward(int comparedTid, int index, SimilarState state2) {
        int comparedTid3;
        SimilarState state3;
        while (index != candidateInfo.size()-1){
            comparedTid3 = candidateInfo.get(index + 1);
            state3 = getSimilarState(comparedTid3);
            if (Double.compare(state2.distance, state3.distance) > 0) {
                candidateInfo.set(index, comparedTid3);
            }else {
                candidateInfo.set(index, comparedTid);
                return index;
            }
            index++;
        }
        candidateInfo.set(index, comparedTid);
        return index;
    }

    /**
     * 判断本轨迹是否有在pruneArea外部的采样点，有返回true，没有返回false
     */
    public boolean outSideRectangle(Rectangle pruneArea) {
        for (Segment elem : trajectory.elems) {
            if (!pruneArea.isInternal(elem.rect))
                return true;
        }
        return false;
    }

    public void clear() {
        candidateInfo.clear();
        relatedInfo.clear();
        rect.extendLength(-threshold);
        threshold = 0.0;
    }
}





















