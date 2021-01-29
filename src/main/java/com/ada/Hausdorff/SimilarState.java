package com.ada.Hausdorff;


import com.ada.common.ArrayQueue;
import com.ada.common.Constants;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * 相似度计算的中间结果,用于增量计算。
 */
@Setter
@Getter
public class SimilarState implements Comparable<SimilarState>, Cloneable, Serializable {
    //比较轨迹,对应二维表格中的行
    public int comparingTID;
    //被比较轨迹,对应二维表格中的列
    public int comparedTID;

    /**
     * 每一行的最小点距：tuple.f0 是点距，tuple.f1行号
     */
    public ArrayQueue<NOAndDistance> row;

    /**
     * 每一列的最小点距：tuple.f0 是点距，tuple.f1行号
     */
    public ArrayQueue<NOAndDistance> col;

    public double distance;

    public SimilarState() {
    }

    public SimilarState(int comparingTID, int comparedTID, ArrayQueue<NOAndDistance> row, ArrayQueue<NOAndDistance> col) {
        this.comparingTID = comparingTID;
        this.comparedTID = comparedTID;
        this.row = row;
        this.col = col;
        if (row != null && col != null)
            setDistance();
    }

    public void setDistance(){
        distance = Double.MIN_VALUE;
        for (NOAndDistance nd : row) distance = Math.max(distance, nd.distance);
        for (NOAndDistance nd : col) distance = Math.max(distance, nd.distance);
    }

    public int getStateAnoTID(int TID){
        if (comparingTID == TID) {
            return comparedTID;
        }else {
            return comparedTID;
        }
    }


    public SimilarState convert(){
        int tmp = comparedTID;
        comparedTID = comparingTID;
        comparingTID = tmp;
        return this;
    }

    public static boolean isEquals(SimilarState state0, SimilarState state1){
        if (state0 == null && state1 == null) return true;
        if (state0 == null || state1 == null) return false;
        if (state0.comparingTID == state1.comparedTID && state0.comparedTID == state1.comparingTID){
            return state0.row.equals(state1.col) &&
                    state0.col.equals(state1.row) &&
                    Constants.isEqual(state0.distance, state1.distance);
        }else if (state0.comparingTID == state1.comparingTID && state0.comparedTID == state1.comparedTID){
            return state0.row.equals(state1.row) &&
                    state0.col.equals(state1.col) &&
                    Constants.isEqual(state0.distance, state1.distance);
        }else {
            return false;
        }
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
}
