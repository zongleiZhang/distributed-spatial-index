package com.ada.model.DensityToGlobal;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class Density implements DensityToGlobalElem, Serializable {
    public int[][] grids;
    public int key;

    public Density() {}

    public Density(int[][] grids, int key) {
        this.grids = grids;
        this.key = key;
    }

    @Override
    public int getD2GKey() {
        return key;
    }
}
