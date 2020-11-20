package com.ada.Grid;

import com.ada.QBSTree.Elem;

public class GridNode {
//    public Rectangle region;
    public GridRectangle gridRectangle;

    public GridNode() {}

    public GridNode(/*Rectangle rect, */GridRectangle gridRectangle) {
//        this.region = rect;
        this.gridRectangle = gridRectangle;
    }

//    public Rectangle getRegion() {
//        return region;
//    }
//
//    public void setRegion(Rectangle region ) {
//        this.region = region;
//    }

    GridDataNode getLeaf(Elem elem) {
        if (this instanceof GridDirNode){
            GridDirNode dirNode = (GridDirNode) this;
            if (dirNode.child[3] != null){
                if (elem.center.data[0] < dirNode.lonBoundary){
                    if (elem.center.data[1] < dirNode.latBoundary){
                        return dirNode.child[0].getLeaf(elem);
                    }else {
                        return dirNode.child[1].getLeaf(elem);
                    }
                }else {
                    if (elem.center.data[1] < dirNode.latBoundary){
                        return dirNode.child[2].getLeaf(elem);
                    }else {
                        return dirNode.child[3].getLeaf(elem);
                    }
                }
            }else if (dirNode.child[2] != null){
                if (elem.center.data[0] < dirNode.lonBoundary){
                    return dirNode.child[0].getLeaf(elem);
                }else {
                    return dirNode.child[2].getLeaf(elem);
                }
            }else {
                if (elem.center.data[1] < dirNode.latBoundary){
                    return dirNode.child[0].getLeaf(elem);
                }else {
                    return dirNode.child[1].getLeaf(elem);
                }
            }
        }else {
            return (GridDataNode) this;
        }
    }
}
