package com.ada.QBSTree;

import com.ada.trackSimilar.Point;

import java.io.Serializable;

public class ElemRoot extends Point implements Cloneable ,Serializable {
    public RCDataNode leaf;

    public ElemRoot() { }

    public ElemRoot(RCDataNode leaf, double[] data) {
        super(data);
        this.leaf = leaf;
//        this.center = center;
    }

    public RCDataNode getLeaf() {
        return leaf;
    }

    public void setLeaf(RCDataNode leaf) {
        this.leaf = leaf;
    }

    public boolean check(){
        if (!leaf.elms.contains(this))
            return false;
        RCNode node = leaf;
        while (!node.isRoot()){
            if (node.parent.child[node.position] != node)
                return false;
            node = node.parent;
        }
        return leaf.tree.root == node;
    }

    @Override
    public ElemRoot clone() {
        ElemRoot elemRoot = (ElemRoot) super.clone();
        elemRoot.leaf = null;
        return elemRoot;
    }

}
