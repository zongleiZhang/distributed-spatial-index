package com.ada.geometry;

import com.ada.Hausdorff.SimilarState;
import com.ada.common.ArrayQueue;
import com.ada.globalTree.GDataNode;
import com.ada.globalTree.GNode;
import com.ada.QBSTree.RCDataNode;
import com.ada.common.Constants;
import com.ada.common.SortList;
import org.apache.flink.api.java.tuple.*;

import java.util.*;

public class TrackKeyTID extends TrackHauOne {
    public List<GDataNode> passP;
    public SortList<GLeafAndBound> topKP;
    public Tuple2<GNode, Double> enlargeTuple;

    public TrackKeyTID(){}

    @SuppressWarnings("rawtypes")
    public TrackKeyTID(RCDataNode leaf,
                       double[] data,
                       Rectangle rect,
                       ArrayQueue<Segment> elms,
                       int TID,
                       List<Integer> candidateInfo,
                       Map<SimilarState, SimilarState> relatedInfo){
        super(leaf, data, rect, elms, TID, candidateInfo, relatedInfo);
        passP = new ArrayList<>();
        topKP = new SortList<>();
        enlargeTuple = new Tuple2<>();
    }

    public List<GLeafAndBound> minusTopKP() {
        List<GLeafAndBound> list = topKP.getList();
        List<GLeafAndBound> res = new ArrayList<>();
        GLeafAndBound minGB = null;
        int i = list.size() - 1;
        while(list.get(i).bound > threshold){
            minGB = list.remove(i);
            res.add(minGB);
            i--;
        }
        assert minGB != null;
        enlargeTuple.f0 = minGB.leaf;
        enlargeTuple.f1 = minGB.bound;
        return res;
    }

    /**
     * 轨迹没有topKP了，将轨迹的topK候选减少
     */
    public <T extends  TrackHauOne> void cutOffCandidate(Map<Integer, T> trackMap) {
        int k = (int) (Constants.topK*1.5);
        while (candidateInfo.size() > k)
            removeICandidate(k, trackMap);
    }

    /**
     * 轨迹没有topKP了，将轨迹的topK候选减少
     */
    public <T extends  TrackHauOne> void cutOffCandidate(Map<Integer, T> trackMap, int notRemove, List<Integer> removeRI) {
        int k = (int) (Constants.topK*1.5);
        while (candidateInfo.size() > k) {
            Integer comparedTID = candidateInfo.remove(k-1);
            T comparedTrack = trackMap.get(comparedTID);
            if (!comparedTrack.candidateInfo.contains(trajectory.TID)){
                if (comparedTID == notRemove){
                    removeRI.add(trajectory.TID);
                }else {
                    comparedTrack.removeRelatedInfo(trajectory.TID);
                }
                removeRelatedInfo(comparedTID);
            }
        }
    }
}
