package com.ada.QBSTree;

import com.ada.common.Constants;
import com.ada.common.collections.Collections;
import com.ada.geometry.Rectangle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DualRootTree<T extends RectElem> implements Index<T>{
//    private RCtree<T> innerTree;
//    private RCtree<T> outerTree;
//    private Rectangle innerRegion;
//    private Rectangle outerRegion;
//    private Rectangle OSR;

    private RCtree<T> verifyTree;
    private Rectangle verifyRegion;

    public DualRootTree(int lowBound, int balanceFactor, int precision, Rectangle region, boolean hasTIDs) {
        verifyRegion = region.clone().extendMultiple(1.0);
        verifyTree = new RCtree<>(lowBound, balanceFactor, precision, verifyRegion.clone(), 0, hasTIDs);
    }

    public List<T> getAllElement(){
        List<T> list = new ArrayList<>(verifyTree.root.elemNum);
        verifyTree.root.getAllElement(list);
        return list;
    }

    public void rebuildRoot(List<T> elms, Rectangle region) {
        verifyRegion = region.clone().extendMultiple(1.0);
        verifyTree = new RCtree<>(verifyTree.lowBound, verifyTree.balanceFactor, verifyTree.precision, verifyRegion.clone(), 0, verifyTree.hasTIDs, elms);
    }

    @Override
    public RCDataNode<T> insert(T elem) {
        if (!verifyRegion.isInternal(elem)) {
            verifyRegion.getUnionRectangle(elem);
            verifyRegion.extendMultiple(0.1);
            verifyTree.rebuildRoot(verifyRegion.clone());
        }
        return verifyTree.insert(elem);
    }

    @Override
    public boolean delete(T elem) {
        return verifyTree.delete(elem);
    }

    @Override
    public <M extends RectElem> void alterELem(M oldElem, Rectangle newRegion) {
        verifyTree.alterELem(oldElem, newRegion);
    }

    /**
     * 获取指定区域region内部的轨迹ID集，不包括与边界相交的轨迹ID
     * @param region 指定的区域
     */
    @Override
    public Set<Integer> getInternalNoIPTIDs(Rectangle region) {
        return verifyTree.getInternalNoIPTIDs(region);
    }


    @Override
    public Set<Integer> getInternalTIDs(Rectangle region){
        return verifyTree.getInternalTIDs(region);
    }

    @Override
    public List<Integer> trackInternal(Rectangle MBR) {
        return verifyTree.trackInternal(MBR);
    }
}
