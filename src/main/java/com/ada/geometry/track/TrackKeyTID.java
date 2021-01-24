package com.ada.geometry.track;

import com.ada.Hausdorff.SimilarState;
import com.ada.common.ArrayQueue;
import com.ada.geometry.GLeafAndBound;
import com.ada.geometry.Rectangle;
import com.ada.geometry.Segment;
import com.ada.geometry.track.TrackHauOne;
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
        for (int i = candidateInfo.size(); i > (int) (Constants.topK*1.5); i--) {
            removeICandidate(i, trackMap);
        }
    }
}
