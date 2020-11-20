package com.ada.Grid;

public class GridDirNode extends GridNode{
    public double lonBoundary;
    public double latBoundary;
    public GridNode[] child = new GridNode[4];

    public GridDirNode() {}

    public GridDirNode(/*Rectangle rect, */GridRectangle gridRectangle,double lonBoundary, double latBoundary, GridNode[] child) {
        super(/*rect, */gridRectangle);
        this.lonBoundary = lonBoundary;
        this.latBoundary = latBoundary;
        this.child = child;
    }

    public double getLonBoundary() {
        return lonBoundary;
    }

    public void setLonBoundary(int lonBoundary) {
        this.lonBoundary = lonBoundary;
    }

    public double getLatBoundary() {
        return latBoundary;
    }

    public void setLatBoundary(int latBoundary) {
        this.latBoundary = latBoundary;
    }

    public GridNode[] getChild() {
        return child;
    }

    public void setChild(GridNode[] child) {
        this.child = child;
    }
}
