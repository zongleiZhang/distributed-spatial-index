package com.ada.Xie_function.STRTree;

import com.ada.common.collections.Collections;
import com.ada.geometry.Rectangle;

import java.io.Serializable;
import java.util.List;

public class DirNode extends STRNode implements Serializable {
    List<STRNode> children;

    public DirNode(Rectangle region, List<STRNode> children) {
        super(region);
        this.children = children;
    }

    @Override
    public void search(Rectangle rectangle, List<LeafNode> result) {
       if (rectangle.isInternal(this.region)){
           addAllLeaves(result);
       }else{
           if (this.region.isIntersection(rectangle)){
                children.forEach(child -> {
                    if (child.region.isIntersection(rectangle)) child.search(rectangle, result);
                });
           }
       }
    }

    @Override
    public void addAllLeaves(List<LeafNode> result) {
        children.forEach(child -> child.addAllLeaves(result));
    }

    @Override
    public boolean check() {
        Rectangle MBR = Rectangle.getUnionRectangle(Collections.changeCollectionElem(children, from -> from.region).toArray(new Rectangle[0]));
        if (MBR.equals(region)){
            for (STRNode child : children) {
                if (!child.check())
                    return false;
            }
            return true;
        }else {
            return false;
        }
    }
}
