package com.ada.model.common.result;

import com.ada.geometry.Segment;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Getter
@Setter
public class QueryResult implements Serializable {
    public long queryID;
    public long inputTime;
    public long outputTime;
    public List<Segment> list;

    public QueryResult() {
    }

    public QueryResult(long queryID, long inputTime, long outputTime, List<Segment> list) {
        this.queryID = queryID;
        this.inputTime = inputTime;
        this.outputTime = outputTime;
        this.list = list;
    }
}
