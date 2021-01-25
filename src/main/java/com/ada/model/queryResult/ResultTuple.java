package com.ada.model.queryResult;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Objects;

@Getter
@Setter
public class ResultTuple implements Serializable {
    public int comparedTID;
    public double distance;

    public ResultTuple() {
    }

    public ResultTuple(int comparedTID, double distance) {
        this.comparedTID = comparedTID;
        this.distance = distance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResultTuple that = (ResultTuple) o;
        return comparedTID == that.comparedTID &&
                Double.compare(that.distance, distance) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(comparedTID, distance);
    }
}
