package com.ada.geometry;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Objects;

@Getter
@Setter
public class GridRectangle implements Serializable {
    public GridPoint low;
    public GridPoint high;

    public GridRectangle() {}

    public GridRectangle(GridPoint low, GridPoint high) {
        if (low.x > high.x || low.y > high.y)
            throw new IllegalArgumentException("low is bigger than high.");
        this.low = low;
        this.high = high;
    }

    /**
     * 判断网格gPoint是否在本矩形内或者边上
     */
    public boolean isInternal(GridPoint gPoint) {
        if (low.x > gPoint.x
                || high.x < gPoint.x)
            return false;
        return low.y <= gPoint.y && high.y >= gPoint.y;
    }

    /**
     * 判断矩形gRectangle是否与本矩形相交
     */
    public boolean isIntersection(GridRectangle gRectangle) {
        if (gRectangle == null)
            throw new IllegalArgumentException("Rectangle cannot be null.");
        if ( low.x > gRectangle.high.x
                || gRectangle.low.x > high.x )
            return false;
        return low.y <= gRectangle.high.y
                && gRectangle.low.y <= high.y;
    }

//    /**
//     * 判断矩形gRectangle是否在本矩形内部
//     */
//    public boolean isInternal(GridRectangle gRectangle) {
//        if (gRectangle == null)
//            throw new IllegalArgumentException("Rectangle cannot be null.");
//        // 只要传入的rectangle有一个维度的坐标越界了就不被包含
//        if ( low.x > gRectangle.low.x
//                || gRectangle.high.x > high.x )
//            return false;
//        return low.y <= gRectangle.low.y
//                && gRectangle.high.y <= high.y;
//    }

    public static GridRectangle rectangleToGridRectangle(Rectangle rectangle){
        GridPoint low = GridPoint.pointToGridPoint(rectangle.low, true);
        GridPoint high = GridPoint.pointToGridPoint(rectangle.high, false);
        return new GridRectangle(low, high);
    }

    public Rectangle toRectangle(){
        Point low = this.low.toPoint(false);
        Point high = this.high.toPoint(true);
        return new Rectangle(low, high);
    }

    @Override
    public String toString() {
        return "GridRectangle{" +
                "low=" + low +
                ", high=" + high +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GridRectangle)) return false;
        GridRectangle that = (GridRectangle) o;
        return low.equals(that.low) &&
                high.equals(that.high);
    }

    @Override
    public int hashCode() {
        return Objects.hash(low, high);
    }


    public static boolean gridRectangleEquals(GridRectangle curRectangle, GridRectangle orgRectangle) {
        if (curRectangle == null && orgRectangle == null)
            return true;
        else if (curRectangle == null || orgRectangle == null)
            return false;
        else
            return curRectangle.equals(orgRectangle);
    }
}
