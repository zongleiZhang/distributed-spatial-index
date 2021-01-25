package com.ada.model.queryResult;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

@Getter
@Setter
public class QueryResult implements Serializable {
    public int TID;
    public ResultTuple[] results;

    public QueryResult() {
    }

    public QueryResult(int TID, ResultTuple[] results) {
        this.TID = TID;
        this.results = results;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryResult that = (QueryResult) o;
        return TID == that.TID &&
                Arrays.equals(results, that.results);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(TID);
        result = 31 * result + Arrays.hashCode(results);
        return result;
    }
}
