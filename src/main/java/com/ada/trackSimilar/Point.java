package com.ada.trackSimilar;

import com.ada.common.Constants;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;

@Getter
@Setter
public class Point implements Serializable, Cloneable {
    public double[] data;

    public Point(){
        data = new double[2];
    }

    public Point(double[] data){
        this.data = data;
    }

    public Point(double x, double y){
        data = new double[]{x,y};
    }


    @Override
    public Point clone()  {
        Point point = null;
        try {
            point = (Point) super.clone();
            if (data != null)
                point.data = data.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return point;
    }

    @Override
    public String toString() {
        return "(" + Constants.df.format(data[0]) + ", " + Constants.df.format(data[1])  + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Point)) return false;
        Point point = (Point) o;
        return Constants.isEqual(data[0], point.data[0]) && Constants.isEqual(data[1], point.data[1]);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getData());
    }

    public double distancePoint(Point trackPoint) {
        return  Math.sqrt(Math.pow(this.data[0]- trackPoint.data[0],2.0)+ Math.pow(this.data[1]- trackPoint.data[1],2.0));
    }

}
