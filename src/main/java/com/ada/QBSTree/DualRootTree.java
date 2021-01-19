package com.ada.QBSTree;

import com.ada.common.Constants;
import com.ada.common.collections.Collections;
import com.ada.geometry.Rectangle;
import com.ada.geometry.TrackInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DualRootTree<T extends ElemRoot> extends RCtree<T> {
    public RCNode<T> outerRoot;

    public DualRootTree(int lowBound, int balanceFactor, int precision, Rectangle centerRegion, Rectangle outerCenterRegion, boolean hasTIDs) {
        super(lowBound, balanceFactor, precision, centerRegion, 0,hasTIDs);
        outerRoot = new RCDataNode<>(0, null, -1, outerCenterRegion, null, new ArrayList<>(), 0, this, new ArrayList<>());
    }

    public RCDataNode<T> insert(T elem) {
        RCDataNode<T> leafNode;
        if (root.centerRegion.isInternal(elem))
            leafNode = root.chooseLeafNode(elem);
        else
            leafNode = outerRoot.chooseLeafNode(elem);
        leafNode.insert();
        return leafNode;
    }

    /**
     * 获取指定区域region内部的轨迹ID集，不包括与边界相交的轨迹ID
     * @param region 指定的区域
     */
    public Set<Integer> getRegionInternalTIDs(Rectangle region) {
        Set<Integer> allTIDs = new HashSet<>();
        try {
            Rectangle innerRect = root.centerRegion.clone();
            Set<Integer> intersections = new HashSet<>();
            if (innerRect.extendLength(500).isIntersection(region)){ //region和内tree负责的区域有交集
                root.getRegionTIDs(region, allTIDs, intersections);
                if (!innerRect.extendLength(-1000).isInternal(region)){ //region和外tree负责的区域也有交集
                    outerRoot.getRegionTIDs(region, allTIDs, intersections);
                }
                allTIDs.removeAll(intersections);
            }else {//region只在外tree中
                outerRoot.getRegionTIDs(region, allTIDs, intersections);
            }
            allTIDs.removeAll(intersections);
        }catch (Exception e){
            e.printStackTrace();
        }
        return allTIDs;
    }

    public void rebuildRoot(Rectangle newRectangle, List<T> innerElms, List<T> outerElms, Rectangle innerMBR, Rectangle outerMBR) {
        boolean flag = false;
        if (hasTIDs) {
            flag = true;
            hasTIDs = false;
        }
        RCDataNode<T> innerLeaf = new RCDataNode<>(0,null,-1, newRectangle, innerMBR, new ArrayList<>()
                , innerElms.size(),this, innerElms);
        root = innerLeaf.recursionSplit();
        RCDataNode<T> outerLeaf = new RCDataNode<>(0,null,-1, outerRoot.centerRegion, outerMBR, new ArrayList<>()
                , outerElms.size(),this, outerElms);
        outerRoot = outerLeaf.recursionSplit();
        if (flag){
            hasTIDs = true;
            List<RCDataNode<T>> leaves = new ArrayList<>();
            root.getLeafNodes(leaves);
            for (RCDataNode<T> leaf : leaves) {
                leaf.TIDs = new HashSet<>(Collections.changeCollectionElem(leaf.elms, t -> ((TrackInfo) t).obtainTID()));
            }
            leaves.clear();
            outerRoot.getLeafNodes(leaves);
            for (RCDataNode<T> leaf : leaves) {
                leaf.TIDs = new HashSet<>(Collections.changeCollectionElem(leaf.elms, t -> ((TrackInfo) t).obtainTID()));
            }
        }
    }
}
