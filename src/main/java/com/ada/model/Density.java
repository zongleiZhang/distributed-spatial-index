package com.ada.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class Density implements DensityToGlobalElem, Serializable {
    public int[][] grids;
    public int key;
    public long timeStamp;

    public Density() {}

    public Density(int[][] grids, int key, long timeStamp) {
        this.grids = grids;
        this.key = key;
        this.timeStamp = timeStamp;
    }

    @Override
    public Integer getDensityToGlobalKey() {
        return key;
    }
}
