package com.ada.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class Density implements DensityToGlobalInt, Serializable {
    public int[][] grids;
    public int key;

    public Density() {}

    public Density(int[][] grids, int key) {
        this.grids = grids;
        this.key = key;
    }

    @Override
    public Integer getDensityToGlobalKey() {
        return key;
    }
}
