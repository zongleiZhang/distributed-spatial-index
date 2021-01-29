package com.ada.QBSTree;

import com.ada.common.Constants;
import com.ada.geometry.Rectangle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DualRootTree<T extends RectElem> implements Index<T>{
    private RCtree<T> innerTree;
    private RCtree<T> outerTree;
    private Rectangle innerRegion;
    private Rectangle outerRegion;
    private Rectangle OSR;

    public DualRootTree(int lowBound, int balanceFactor, int precision, Rectangle region, boolean hasTIDs) {
        this.innerRegion = region.clone().extendMultiple(0.075);
        this.outerRegion = region.clone().extendMultiple(0.35);
        this.OSR = region.clone().shrinkMultiple(0.1);
        this.innerTree = new RCtree<>(lowBound, balanceFactor, precision, innerRegion.clone(), 0, hasTIDs);
        this.outerTree = new RCtree<>(lowBound, balanceFactor, precision, outerRegion.clone(), 0, hasTIDs);
    }

    public void rebuildRoot(List<T> innerElms,
                            List<T> outerElms,
                            Rectangle innerRegion,
                            Rectangle outerRegion,
                            Rectangle OSR) {
        for (T innerElm : innerElms) {
            if (!innerRegion.isInternal(innerElm.rect))
                throw new IllegalArgumentException();
        }
        for (T outerElm : outerElms) {
            if (!(outerRegion.isInternal(outerElm.rect) && !OSR.isIntersection(outerElm.rect)))
                throw new IllegalArgumentException();
        }
        if (outerRegion.isInternal(OSR))
            throw new IllegalArgumentException();
        this.innerRegion = innerRegion;
        this.outerRegion = outerRegion;
        this.OSR = OSR;
        innerTree = new RCtree<>(innerTree.lowBound, innerTree.balanceFactor, innerTree.precision, innerRegion, 0, innerTree.hasTIDs, innerElms);
        outerTree = new RCtree<>(outerTree.lowBound, outerTree.balanceFactor, outerTree.precision, outerRegion, 0, outerTree.hasTIDs, outerElms);
    }

    @Override
    public RCDataNode<T> insert(T elem) {
        if (innerRegion.isInternal(elem.rect)) {
            return innerTree.insert(elem);
        }else {
            if (!outerRegion.isInternal(elem)){
                outerRegion.getUnionRectangle(elem);
                outerRegion.extendMultiple(0.05);
                outerTree.rebuildRoot(outerRegion);
            }
            shrinkOSR(OSR, elem);
            return outerTree.insert(elem);
        }
    }

    public static <T extends RectElem> void shrinkOSR(Rectangle OSR, T elem) {
        double olehX = OSR.low.data[0] - elem.rect.high.data[0];
        double elohX = elem.rect.low.data[0] - OSR.high.data[0];
        double olehY = OSR.low.data[1] - elem.rect.high.data[1];
        double elohY = elem.rect.low.data[1] - OSR.high.data[1];
        if (olehX <= Constants.zero && elohX <= Constants.zero &&
                olehY <= Constants.zero && elohY <= Constants.zero){
            double max = Math.max(Math.max(Math.max(olehX, elohX), olehY), elohY);
            if (max == olehX){
                OSR.low.data[0] -= olehX;
            }else if (max == elohX){
                OSR.high.data[0] += elohX;
            }else if (max == olehY){
                OSR.low.data[1] -= olehY;
            }else {
                OSR.high.data[1] += elohY;
            }
        }
    }

    @Override
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
    @Override
    public Set<Integer> getInternalNoIPTIDs(Rectangle region) {
        Set<Integer> allTIDs = new HashSet<>();
        Set<Integer> intersections = new HashSet<>();
        innerTree.root.getRegionTIDs(region, allTIDs, intersections);
        if (!OSR.isInternal(region)){
            outerTree.root.getRegionTIDs(region, allTIDs, intersections);
        }
        allTIDs.removeAll(intersections);
        return allTIDs;
    }


    @Override
    public Set<Integer> getInternalTIDs(Rectangle region){
        Set<Integer> allTIDs = new HashSet<>();
        innerTree.root.getRegionTIDs(region, allTIDs);
        if (!innerRegion.isInternal(region)){
            outerTree.root.getRegionTIDs(region, allTIDs);
        }
        return allTIDs;
    }

    @Override
    public List<Integer> trackInternal(Rectangle MBR) {
        List<Integer> TIDs = new ArrayList<>();
        innerTree.root.trackInternal(MBR, TIDs);
        if (!OSR.isIntersection(MBR)) {
            outerTree.root.trackInternal(MBR, TIDs);
        }
        return TIDs;
    }

    @Override
    public <M extends RectElem> void alterELem(M oldElem, Rectangle newRegion) {
        if (oldElem.leaf.tree == innerTree){
            innerTree.alterELem(oldElem, newRegion);
        }else {
            outerTree.alterELem(oldElem, newRegion);
        }
    }
}
