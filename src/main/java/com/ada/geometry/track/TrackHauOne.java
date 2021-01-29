package com.ada.geometry.track;

import com.ada.Hausdorff.Hausdorff;
import com.ada.Hausdorff.SimilarState;
import com.ada.QBSTree.RCDataNode;
import com.ada.QBSTree.RectElem;
import com.ada.common.ArrayQueue;
import com.ada.common.Constants;
import com.ada.geometry.Rectangle;
import com.ada.geometry.Segment;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.*;

@Getter
@Setter
public class TrackHauOne extends RectElem implements Serializable {
    public Trajectory trajectory;
    public List<Integer> candidateInfo;
    private Map<SimilarState, SimilarState> relatedInfo;
    public double threshold;

    public TrackHauOne(){}

    @SuppressWarnings("rawtypes")
    public TrackHauOne(RCDataNode leaf,
                       double[] data,
                       Rectangle rect,
                       ArrayQueue<Segment> elms,
                       int TID,
                       List<Integer> candidateInfo,
                       Map<SimilarState, SimilarState> relatedInfo) {
        super(leaf, data, rect);
        trajectory = new Trajectory(elms, TID);
        this.candidateInfo = candidateInfo;
        this.relatedInfo = relatedInfo;
    }

    public SimilarState putRelatedInfo(SimilarState state){
        SimilarState oldState = relatedInfo.get(state);
        if (oldState == null){
            state.convert();
            oldState = relatedInfo.get(state);
            state.convert();
            if(oldState == null){
                relatedInfo.put(state,state);
                return state;
            }else {
                if (!Constants.isEqual(oldState.distance, state.distance))
                    System.out.print("");
                return oldState;
            }
        }else {
            if (!Constants.isEqual(oldState.distance, state.distance))
                System.out.print("");
            return oldState;
        }

    }

    /**
     * 向轨迹track中添加一个新的候选轨迹comparedTrack
     */
    public void addTrackCandidate(TrackHauOne comparedTrack) {
        Integer comparedTID = comparedTrack.trajectory.TID;
        SimilarState state = getSimilarState(comparedTID);
        if (state == null) {
            state = Hausdorff.getHausdorff(trajectory, comparedTrack.trajectory);
            putRelatedInfo(state);
            comparedTrack.putRelatedInfo(state);
        }
        candidateInfo.add(comparedTID);
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

    /**
     * 用指定的阈值threshold计算轨迹的裁剪域。
     */
    public Rectangle getPruningRegion(double threshold){
        Rectangle rectangle = null;
        boolean flag = true;
        for (Segment s : trajectory.elms) {
            if (flag){
                flag = false;
                rectangle = s.rect.clone();
            }else {
                rectangle = rectangle.getUnionRectangle(s.rect);
            }
        }
        return rectangle.extendLength(threshold);
    }

    /**
     * 轨迹track添加一些候选轨迹newCandidate
     */
    public <T extends TrackHauOne> void trackAddCandidates(Set<Integer> newCandidate,
                                                           Map<Integer, T> trackMap) {
        for (Integer comparedTid : newCandidate) {
            SimilarState state = getSimilarState(comparedTid);
            if (state == null) {
                TrackHauOne comparedTrack = trackMap.get(comparedTid);
                state = Hausdorff.getHausdorff(trajectory, comparedTrack.trajectory);
                state = comparedTrack.putRelatedInfo(state);
                putRelatedInfo(state);
            }
            candidateInfo.add(comparedTid);
            updateCandidateInfo(comparedTid);
        }
    }

    /**
     * 移除第i个候选轨迹，i从1计。
     * 待优化
     */
    public <T extends TrackHauOne> void removeICandidate(int i, Map<Integer, T> map){
        Integer comparedTID = candidateInfo.remove(i-1);
        T t = map.get(comparedTID);
        if (t == null)
            System.out.print("");
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
            if (candidateInfo.size() == 0)
                System.out.print("");
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
            if (state1 == null || state2 == null)
                System.out.print("");
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
     * 待优化
     */
    public boolean outSideRectangle(Rectangle pruneArea) {
        for (Segment elem : trajectory.elms) {
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





















