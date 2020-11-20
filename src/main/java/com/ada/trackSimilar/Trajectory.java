package com.ada.trackSimilar;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Trajectory继承RectElem，RectElem的rect域是Trajectory对象的裁剪域
 */
public class Trajectory <T extends TrackInfo> implements Serializable,TrackInfo {
    public LinkedList<T> elems;
    public Integer TID;


    public Trajectory(){}

    public Trajectory(LinkedList<T> elems,
                      int TID){
        this.elems = elems;
        this.TID = TID;
    }

    public LinkedList<T> getElems() {
        return elems;
    }

    public void setElems(LinkedList<T> elems) {
        this.elems = elems;
    }

    public Integer getTID() {
        return TID;
    }

    public void setTID(Integer TID) {
        this.TID = TID;
    }

    public void addElem(T t){
        elems.add(t);
    }

    public void addElems(List<T> ts){
        this.elems.addAll(ts);
    }

    @Override
    public TrackMessage toMessage(){
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Trajectory)) return false;
        Trajectory<T> that = (Trajectory<T>) o;
        return TID.equals(that.TID);
    }

    @Override
    public int hashCode() {
        return TID.hashCode();
    }

    /**
     * 移除过时采样点
     * @param time 时间标准
     * @return 0 本轨迹的所有采样点都过时， 1移除过时采样点后还有超过一个元素保留
     */
    public List<T> removeElem(long time){
        List<T> timeOutElem = new ArrayList<>();
        while(!elems.isEmpty() && elems.getFirst().obtainTimeStamp() < time)
            timeOutElem.add(elems.removeFirst());
        return timeOutElem;
    }

//    /**
//     * 用指定的阈值threshold计算轨迹的裁剪域。
//     */
//    public Rectangle getPruningRegion(double threshold) throws CloneNotSupportedException {
//        Rectangle rectangle = null;
//        if (elems.getFirst() instanceof Segment){
//            List<Segment> ss = (List<Segment>) elems;
//            boolean flag = true;
//            for (Segment s : ss) {
//                if (flag){
//                    flag = false;
//                    rectangle = s.rect.clone();
//                }else {
//                    rectangle = rectangle.getUnionRectangle(s.rect);
//                }
//            }
//        }else {
//            List<TrackPointElem> ss = (List<TrackPointElem>) elems;
//            rectangle = Rectangle.pointsMBR(ss.toArray(new Point[0]));
//        }
//        assert rectangle != null;
//        return rectangle.extendLength(threshold);
//    }
//
//    public void initCache(Collection<Trajectory<T>> tracks) {
//        for (Trajectory<T> comparedTrack : tracks) {
//            SimilarState state = Constants.getDTW((Trajectory<TrackPointElem>) this, (Trajectory<TrackPointElem>)comparedTrack);
//            if (state == null) {
//                state = Constants.getHausdorff((Trajectory<Segment>) this, (Trajectory<Segment>) comparedTrack);
//                this.relatedInfo.put(state, state);
//                comparedTrack.relatedInfo.put(state, state);
//            }
//            this.candidateInfo.add(comparedTrack.TID);
//        }
//        sortCandidateInfo();
//    }

    @Override
    public int obtainTID() {
        return TID;
    }

    @Override
    public long obtainTimeStamp() {
        return elems.getFirst().obtainTimeStamp();
    }
}















