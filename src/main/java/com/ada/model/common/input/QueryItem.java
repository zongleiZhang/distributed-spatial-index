package com.ada.model.common.input;

import com.ada.geometry.Rectangle;
import com.ada.model.common.input.InputItem;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class QueryItem implements InputItem, Serializable {
    public long queryID;
    public long timeStamp;
    public long inputTime;
    public Rectangle rect;

    public QueryItem() {}

    public QueryItem(long queryID, long timeStamp, long inputTime, Rectangle rect) {
        this.rect = rect;
        this.timeStamp = timeStamp;
        this.inputTime = inputTime;
        this.queryID = queryID;
    }

    @Override
    public Integer getD2GKey() {
        return (int) queryID%Integer.MAX_VALUE;
    }

    @Override
    public int getInputKey() {
        return (int) queryID%Integer.MAX_VALUE;
    }
}
