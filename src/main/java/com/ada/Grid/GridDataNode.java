package com.ada.Grid;

public class GridDataNode extends GridNode{
    public int x;
    public int y;

    public GridDataNode() { }

    public GridDataNode(/*Rectangle rect,*/ GridRectangle gridRectangle) {
        super(/*rect,*/ gridRectangle);
        this.x = gridRectangle.low.x;
        this.y = gridRectangle.low.y;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }
}
