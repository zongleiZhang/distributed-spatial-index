package com.ada.trackSimilar;


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
     * 行中间结果
     * 对于DTW：tuple.f0 是点距 tuple.f1是DTW步长
     * 对于Hausdorff: tuple.f0 是点距 tuple.f1是选取comparedTrack的第几个点
     */
    public Tuple2<Double, Integer>[] row;

    /**
     * 列中间结果
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
        if(Constants.similar.equals("DTW")) {
            distance = row[row.length - 1].f0 / row[row.length - 1].f1;
        }else {
            distance = row[0].f0;
            for (int i = 1; i < row.length; i++)
                distance = Math.max(distance, row[i].f0);
            for (Tuple2<Double, Integer> tuple2 : col)
                distance = Math.max(distance, tuple2.f0);
        }

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

    /**
     * 比较或者被比较的轨迹发生增量变化后，更新相似度计算的中间结果
     * @param row 变化后的行
     * @param col 变化后的列
     * @param compareType 相似度度量DTW或Hausdorff
     */
    public void alterCompareTrack(Tuple2<Double, Integer>[] row, Tuple2<Double, Integer>[] col, String compareType){
        switch (compareType){
            case "DTW":
                this.row = row;
                this.col = col;
                break;
            case "Hausdorff":
                break;
            default:
                throw new IllegalArgumentException("Error argument.");
        }
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
}
