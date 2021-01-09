package com.ada.geometry;


import com.ada.common.Constants;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Objects;

/**
 * 相似度计算的中间结果,用于增量计算。
 */
public class SimilarState implements Comparable<SimilarState>, Cloneable, Serializable {
    //比较轨迹,对应二维表格中的行
    public int comparingTID;
    //被比较轨迹,对应二维表格中的列
    public int comparedTID;

    /**
     * 每一行的最小点距：tuple.f0 是点距，tuple.f1行号
     */
    public Tuple2<Double, Integer>[] row;

    /**
     * 每一列的最小点距：tuple.f0 是点距，tuple.f1行号
     */
    public Tuple2<Double, Integer>[] col;

    public double distance;

    public SimilarState() {
    }

    public SimilarState(int comparingTID, int comparedTID, Tuple2<Double, Integer>[] row, Tuple2<Double, Integer>[] col) {
        this.comparingTID = comparingTID;
        this.comparedTID = comparedTID;
        this.row = row;
        this.col = col;
        if (row != null && col != null)
            setDistance();
    }

    public void setDistance(){
        distance = row[0].f0;
        for (int i = 1; i < row.length; i++)
            distance = Math.max(distance, row[i].f0);
        for (Tuple2<Double, Integer> tuple2 : col)
            distance = Math.max(distance, tuple2.f0);
    }


    public SimilarState convert(){
        int tmp = comparedTID;
        comparedTID = comparingTID;
        comparingTID = tmp;
        return this;
    }


    @Override
    public int compareTo(SimilarState o) {
        return Double.compare(distance,o.distance);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SimilarState)) return false;
        SimilarState state = (SimilarState) o;
        return comparingTID == state.comparingTID && comparedTID == state.comparedTID;
    }

    @Override
    public int hashCode() {
        return Objects.hash(comparingTID, comparedTID);
    }

    public void update(Tuple2<Double, Integer>[] row, Tuple2<Double, Integer>[] col) {
        this.row = row;
        this.col = col;
        setDistance();
    }

    public void update(Tuple2<Double, Integer>[] row, Tuple2<Double, Integer>[] col, double distance) {
        this.row = row;
        this.col = col;
        this.distance = distance;
    }
}
