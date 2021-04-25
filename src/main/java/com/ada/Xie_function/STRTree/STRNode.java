package com.ada.Xie_function.STRTree;

import com.ada.geometry.Rectangle;

import java.io.Serializable;
import java.util.List;

public abstract class STRNode implements Serializable {
    Rectangle region;

    public STRNode(Rectangle region) {
        this.region = region;
    }

    @Override
    public String toString() {
        if (region == null)
            return null;
        else
            return region.toString();
    }

    public abstract void search(Rectangle rectangle, List<LeafNode> result);

    public abstract void addAllLeaves(List<LeafNode> result);

    public abstract boolean check();
}
