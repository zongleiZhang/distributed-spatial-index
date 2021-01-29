package com.ada.QBSTree;

import com.ada.geometry.Rectangle;

import java.util.List;
import java.util.Set;

public interface Index<T extends ElemRoot> {

    RCDataNode<T> insert(T elem);
    boolean delete(T elem);

    /**
     * 获取指定区域region内部的轨迹ID集，不包括与边界相交的轨迹ID
     */
    Set<Integer> getInternalNoIPTIDs(Rectangle region);

    /**
     * 获取索引中其矩形包含矩形MBR在内的索引项矩形
     */
    List<Integer> trackInternal(Rectangle MBR);

    /**
     * 获取指定的矩形区域内包含的轨迹集合,不去除与边界相交的轨迹ID
     * @param region 指定的矩形区域
     */
    Set<Integer> getInternalTIDs(Rectangle region);

    /**
     * 索引矩形oldElem发生变化，更新其在索引中的位置。
     */
    <M extends RectElem> void alterELem(M oldElem, Rectangle newRegion);
}
