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
    public long timeStamp;
    public List<Segment> list;

    public QueryResult() {
    }

    public QueryResult(long queryID, long timeStamp, List<Segment> list) {
        this.queryID = queryID;
        this.timeStamp = timeStamp;
        this.list = list;
    }
}
