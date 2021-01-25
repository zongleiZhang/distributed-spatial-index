package com.ada.QBSTree;

import com.ada.common.collections.Collections;
import com.ada.geometry.Rectangle;
import com.ada.geometry.TrackInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DualRootTree<T extends RectElem> {
    public RCtree<T> innerTree;
    public RCtree<T> outerTree;
    public Rectangle innerRegion;
    public Rectangle outerRegion;

    public DualRootTree(int lowBound, int balanceFactor, int precision, Rectangle innerRegion, Rectangle outerRegion, boolean hasTIDs) {
        this.innerTree = new RCtree<>(lowBound, balanceFactor, precision, innerRegion, 0, hasTIDs);
        this.outerTree = new RCtree<>(lowBound, balanceFactor, precision, outerRegion, 0, hasTIDs);
        this.innerRegion = innerRegion;
        this.outerRegion = outerRegion;
    }

    public RCDataNode<T> insert(T elem) {
        if (innerRegion.isInternal(elem.rect)) {
            return innerTree.insert(elem);
        }else {
            return outerTree.insert(elem);
        }
    }

    public boolean delete(T elem) {
        @SuppressWarnings("unchecked")
        RCDataNode<T> leafNode = elem.leaf;
        if (leafNode == null) {
            return false;
        } else {
            elem.leaf = null;
            return leafNode.delete(elem);
        }
    }

    /**
     * 获取指定区域region内部的轨迹ID集，不包括与边界相交的轨迹ID
     * @param region 指定的区域
     */
    public Set<Integer> getRegionInternalTIDs(Rectangle region) {
        Set<Integer> allTIDs = new HashSet<>();
        try {
            Set<Integer> intersections = new HashSet<>();
            if (innerRegion.isIntersection(region)){ //region和内tree负责的区域有交集
                innerTree.root.getRegionTIDs(region, allTIDs, intersections);
            }
            outerTree.root.getRegionTIDs(region, allTIDs, intersections);
            allTIDs.removeAll(intersections);
        }catch (Exception e){
            e.printStackTrace();
        }
        return allTIDs;
    }

    public void rebuildRoot(Rectangle newRectangle, List<T> innerElms, List<T> outerElms, Rectangle innerMBR, Rectangle outerMBR) {
        this.innerRegion = innerMBR;
        this.outerRegion = outerMBR;
        this.innerTree = new RCtree<>();
        this.outerTree = new RCtree<>();

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
