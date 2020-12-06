package com.ada.Grid;

import com.ada.common.Constants;
import com.ada.geometry.Point;

import java.util.Objects;

/**
 * 网格点，一个点表示一个网格
 */
public class GridPoint {
    public int x;
    public int y;

    public GridPoint() { }

    public GridPoint(int x, int y) {
        this.x = x;
        this.y = y;
    }

    /**
     * 从Point转换成GridPoint
     * @param point 转换的点
     * @param isFloor 是否向下取整，true向下取整，false向上取整
     * @return 一个GridPoint
     */
    public static GridPoint pointToGridPoint(Point point, boolean isFloor){
        int intMinusX;
        int intMinusY;
        if (isFloor) {
            intMinusX = (int) Math.floor( point.data[0]/(Constants.globalRegion.high.data[0]/(Constants.gridDensity+1)) );
            intMinusY = (int) Math.floor( point.data[1]/(Constants.globalRegion.high.data[1]/(Constants.gridDensity+1)) );
        }else {
            intMinusX = (int) Math.ceil( point.data[0]/(Constants.globalRegion.high.data[0]/(Constants.gridDensity+1)) );
            intMinusY = (int) Math.ceil( point.data[1]/(Constants.globalRegion.high.data[1]/(Constants.gridDensity+1)) );
        }
        return new GridPoint(intMinusX,intMinusY);
    }

    /**
     * 从GridPoint转换成Point
     * @param isAdd true转换成网格的左下点，false转换成网格的右上点
     */
    public Point toPoint(boolean isAdd){
        double x;
        double y;
        if (isAdd){
            x = (this.x+1) * (Constants.globalRegion.high.data[0]/(Constants.gridDensity+1));
            y = (this.y+1) * (Constants.globalRegion.high.data[1]/(Constants.gridDensity+1));
        }else {
            x = this.x * (Constants.globalRegion.high.data[0]/(Constants.gridDensity+1));
            y = this.y * (Constants.globalRegion.high.data[1]/(Constants.gridDensity+1));
        }
        return new Point(x, y);
    }

    @Override
    public String toString() {
        return x +  ", " + y +' ';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GridPoint)) return false;
        GridPoint gridPoint = (GridPoint) o;
        return x == gridPoint.x &&
                y == gridPoint.y;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }
}
