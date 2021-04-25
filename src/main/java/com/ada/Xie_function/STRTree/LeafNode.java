package com.ada.Xie_function.STRTree;

import com.ada.geometry.Rectangle;

import java.io.Serializable;
import java.util.List;

public class LeafNode extends STRNode implements Serializable {

    public int leafID;

    public LeafNode(Rectangle region) {
        super(region);
    }

    @Override
    public void search(Rectangle rectangle, List<LeafNode> result) {
        result.add(this);
    }

    @Override
    public void addAllLeaves(List<LeafNode> result) {
        result.add(this);
    }

    @Override
    public boolean check() {
        return true;
    }
}
